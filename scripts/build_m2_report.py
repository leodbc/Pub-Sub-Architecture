#!/usr/bin/env python3
"""Compila docs/relatorio_m2.md + seis SVGs para DOCX e PDF consistentes.

Requisitos externos: pandoc, LibreOffice (headless), Inkscape; Python: python-docx, PyMuPDF.
Saídas em --outdir (fora do repositório por padrão); não executa experimentos.
O sumário é estático, em português, com páginas obtidas da renderização REAL.
A paginação resultante é específica do motor/versões/fontes utilizados.
"""
from __future__ import annotations
import argparse, hashlib, json, os, re, shutil, subprocess, tempfile, zipfile
from pathlib import Path
import fitz
from docx import Document
from docx.shared import Cm, Pt, RGBColor
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.text import WD_BREAK, WD_TAB_ALIGNMENT, WD_TAB_LEADER
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

TOPICS=[
('1. Identificação',r'^1\. Identificação', '1. Identificação e apresentação'),
('2. Resumo',r'^2\. Resumo', '2. Resumo'),
('3. Fundamentação',r'^3\. Fundamentação', '3. Fundamentação científica'),
('4. Ambiente',r'^4\. Ambiente', '4. Ambiente e reprodutibilidade'),
('5. Experimentos',r'^5\. Experimentos', '5. Experimentos E1–E9'),
('6. Questões comuns',r'^6\. Questões comuns', '6. Questões comuns Q1–Q14'),
('7. Questões específicas',r'^7\. Questões específicas', '7. Questões específicas S1–S4'),
('8. Quadro',r'^8\. Quadro', '8. Quadro-síntese obrigatório'),
('9. Limitações',r'^9\. Limitações', '9. Limitações e ameaças à validade'),
('10. Participação',r'^10\. Participação', '10. Participação individual'),
('11. Uso',r'^11\. Declaração', '11. Uso de IA generativa'),
('12. Referências',r'^12\. Referências', '12. Referências'),
('Anexo A',r'^Anexo A —', 'Anexo A — Proveniência dos experimentos'),
('Anexo B',r'^Anexo B —', 'Anexo B — Matriz de conformidade'),
]
# Médias, perguntas, respostas e tabelas de resultados vêm exclusivamente do Markdown.
TABLE_WIDTHS={
 0:[2.4,3.0,3.5,3.3,2.6],1:[3.1,1.3,2.4,2.4,1.0,4.3],
 2:[3.0,1.5,3.0,3.4],3:[1.3,1.05,2.2,2.0,2.0,1.9,4.25],4:[2.25,1.45,3.7,3.7,3.6],
 5:[2.0,1.15,3.65,3.65,4.25],6:[2.25,4.0,4.0,3.4],
 7:[3.0,5.0,5.0],8:[4.0,10.7],9:[3.0,2.2,2.2,2.2,2.2,2.7],
 10:[3.7,2.8,8.2],
}

def run(cmd, cwd=None, env=None):
 print('+', ' '.join(map(str,cmd)),flush=True)
 subprocess.run(list(map(str,cmd)),cwd=cwd,check=True,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def make_reference(path):
 d=Document();sec=d.sections[0]
 sec.page_height=Cm(29.7);sec.page_width=Cm(21)
 sec.top_margin=Cm(1.7);sec.bottom_margin=Cm(1.55)
 sec.left_margin=Cm(1.9);sec.right_margin=Cm(1.8)
 sec.header_distance=Cm(.65);sec.footer_distance=Cm(.65)
 st=d.styles['Normal'];st.font.name='Liberation Serif';st.font.size=Pt(10.5)
 st.paragraph_format.space_after=Pt(4);st.paragraph_format.line_spacing=1.18
 for name,sz,col,pre,post in [('Title',18,'234E6A',13,8),('Subtitle',12,'436579',4,10),('Heading 1',13,'1C4A67',11,6),('Heading 2',10.4,'1C4A67',8,3),('Heading 3',9.6,'294A60',6,3),('TOC Heading',12,'234E6A',8,5)]:
  st=d.styles[name];st.font.name='Liberation Sans';st.font.size=Pt(sz)
  st.font.color.rgb=RGBColor.from_string(col);st.font.bold=(name!='Subtitle')
  st.paragraph_format.space_before=Pt(pre);st.paragraph_format.space_after=Pt(post);st.paragraph_format.keep_with_next=True
 for name in ['Caption','Table Caption']:
  if name not in d.styles:d.styles.add_style(name,WD_STYLE_TYPE.PARAGRAPH)
  st=d.styles[name];st.font.name='Liberation Serif';st.font.size=Pt(8);st.font.italic=True
  st.paragraph_format.space_before=Pt(2);st.paragraph_format.space_after=Pt(4)
 for name in ['Normal','Body Text']:d.styles[name].paragraph_format.widow_control=True
 h=sec.header.paragraphs[0];h.text='UNIVALI  |  SISTEMAS DISTRIBUÍDOS  |  TRABALHO M2';h.style='Caption'
 f=sec.footer.paragraphs[0];f.text='Relatório M2  •  Kafka KRaft × NATS JetStream     |     Página '
 pg=OxmlElement('w:fldSimple');pg.set(qn('w:instr'),'PAGE');f._p.append(pg);f.style='Caption'
 d.save(path)

def rebuild_tables(d):
 for idx,old in enumerate(list(d.tables)):
  data=[[c.text for c in row.cells] for row in old.rows]
  if not data:continue
  n=len(data[0]);weights=TABLE_WIDTHS.get(idx)
  if weights is None or len(weights)!=n:
   raise RuntimeError(f'Layout de tabela inesperado: índice {idx}, colunas {n}; revisar manualmente antes de atualizar o layout')
  nt=d.add_table(rows=len(data),cols=n);nt.style='Table Grid';nt.autofit=False
  for j,w in enumerate(weights):nt.columns[j].width=Cm(w)
  for i,row in enumerate(nt.rows):
   for j,c in enumerate(row.cells):
    c.width=Cm(weights[j]);c.text=data[i][j]
    for p in c.paragraphs:
     p.paragraph_format.space_after=Pt(0);p.paragraph_format.space_before=Pt(0)
     for r in p.runs:
      r.font.name='Liberation Serif';r.font.size=Pt(7.1 if n>4 else 7.7);r.font.bold=i==0
   trPr=row._tr.get_or_add_trPr();cs=OxmlElement('w:cantSplit');trPr.append(cs)
   if i==0:
    pr=row._tr.get_or_add_trPr();e=OxmlElement('w:tblHeader');e.set(qn('w:val'),'true');pr.append(e)
  old._tbl.addprevious(nt._tbl);old._tbl.getparent().remove(old._tbl)

def build_docx(raw, path, commit):
 d=Document(raw)
 for p in d.paragraphs:
  if p.style.name=='Title':p.paragraph_format.space_before=Pt(120);p.paragraph_format.space_after=Pt(22)
  elif p.style.name=='Subtitle':p.paragraph_format.space_after=Pt(22)
 # Regra editorial: nunca separar título de tabela de suas primeiras linhas;
 # nunca separar figura (desenho) da legenda subsequente.
 for paragraph in d.paragraphs:
  label=paragraph.text.strip()
  # Anexos iniciam em páginas próprias, sem deslocar o quadro-síntese.
  if label.startswith(('Anexo A —', 'Anexo B —')) and paragraph.style.name=='Heading 1':
   paragraph.paragraph_format.page_break_before=True
  # As tabelas longas E5/E6 cabem separadamente em uma página; iniciar uma
  # nova página evita quebra entre o título e as últimas linhas (LibreOffice).
  if re.match(r'^Tabela [45]\s*[—-]', label):
   paragraph.paragraph_format.page_break_before=True
  if re.match(r'^Tabela\s+\d+\s+[—-]',label):
   paragraph.paragraph_format.keep_with_next=True
   paragraph.paragraph_format.keep_together=True
  if paragraph._p.xpath('.//w:drawing'):
   paragraph.paragraph_format.keep_with_next=True
   paragraph.paragraph_format.keep_together=True
  if re.match(r'^Figura\s+\d+\s+[—-]',label):
   paragraph.paragraph_format.keep_together=True
 for img in d.inline_shapes:
  if img.width>Cm(16.3):
   k=Cm(16.3)/img.width;img.width=Cm(16.3);img.height=int(img.height*k)
 # O sumário automático do Pandoc é frequentemente traduzido para 'Table of Contents'
 # e não é recalculado em todos os visualizadores DOCX: removê-lo integralmente.
 body=d.element.body
 for sd in body.findall(qn('w:sdt')):
  body.remove(sd)
 front=next((p for p in d.paragraphs if 'Semestre: 2026/2' in p.text),None)
 if front is None:raise RuntimeError('Capa alterada: parágrafo Semestre não encontrado')
 front.add_run().add_break(WD_BREAK.PAGE)
 first=next((p for p in d.paragraphs if p.style.name=='Heading 1' and p.text.startswith('1.')),None)
 if first is None:raise RuntimeError('Capítulo 1 não encontrado')
 p=first.insert_paragraph_before('SUMÁRIO');p.style='Heading 1'
 for key,pat,label in TOPICS:
  p=first.insert_paragraph_before(label+'\t[PÁGINA]');p.style='Normal'
  p.paragraph_format.space_after=Pt(5)
  p.paragraph_format.tab_stops.add_tab_stop(Cm(15),WD_TAB_ALIGNMENT.RIGHT,WD_TAB_LEADER.DOTS)
  if key.startswith('Anexo'):p.runs[0].font.italic=True
 p=first.insert_paragraph_before();p.add_run().add_break(WD_BREAK.PAGE)
 rebuild_tables(d)
 d.core_properties.title='Trabalho M2 — Mensageria Publish–Subscribe (Kafka KRaft × NATS JetStream)'
 d.core_properties.subject='Relatório acadêmico M2 — UNIVALI'
 d.core_properties.author='Equipe acadêmica — identificação pendente de preenchimento'
 d.core_properties.keywords='Sistemas distribuídos; Kafka; KRaft; JetStream; Mensageria'
 d.core_properties.comments='Fonte docs/relatorio_m2.md; commit '+(commit or 'não informado')
 d.save(path)

def libreoffice_pdf(docx, outdir):
 outdir.mkdir(parents=True,exist_ok=True)
 with tempfile.TemporaryDirectory(prefix='m2_lo_') as ud:
  env=os.environ.copy();env['HOME']=ud
  run(['libreoffice', '-env:UserInstallation=file://'+ud+'/profile', '--headless', '--convert-to','pdf:writer_pdf_Export', '--outdir',outdir,docx],env=env)
 pdf=outdir/(docx.stem+'.pdf')
 if not pdf.exists() or pdf.stat().st_size<1000:raise RuntimeError('PDF não gerado')
 return pdf

def locations(pdf):
 doc=fitz.open(pdf);pages=[]
 for key,reg,label in TOPICS:
  found=[i+1 for i,p in enumerate(doc) if i>1 and re.search(reg,p.get_text(),re.M)]
  if len(found)!=1:raise RuntimeError(f'Âncora de sumário não única {key}: {found}')
  pages.append(found[0])
 return pages

def fill_pages(path,numbers):
 d=Document(path);entries=[p for p in d.paragraphs if '[PÁGINA]' in p.text]
 if len(entries)!=len(numbers):raise RuntimeError('Contagem de entradas do sumário inconsistente')
 for p,pg in zip(entries,numbers):
  for r in p.runs:r.text=r.text.replace('[PÁGINA]',str(pg))
 d.save(path)

def check_document(pdf,docx,source,expected_pages=None,expected_annex=None):
 doc=fitz.open(pdf);pages=locations(pdf)
 if pages[-2]>pages[-1] or pages[-1]>len(doc) or pages[-2]<15 or pages[-2]>26:
  raise RuntimeError(f'Paginação/limite do corpo inesperado: total {len(doc)}, anexos {pages[-2:]}')
 if expected_pages is not None and len(doc)!=expected_pages:raise RuntimeError('Quantidade de páginas mudou')
 if expected_annex is not None and pages[-2]!=expected_annex:raise RuntimeError('Início dos anexos mudou')
 if any(abs(p.rect.width-595.276)>1 or abs(p.rect.height-841.89)>1 for p in doc):
  raise RuntimeError('PDF fora do A4')
 t='\n'.join(p.get_text() for p in doc)
 assert 'SUMÁRIO' in doc[1].get_text() and 'Table of Contents' not in t
 assert 'A verificar no arquivo PDF final' not in t
 assert all(f'Figura {i}' in t for i in range(1,7))
 # Para cada figura, a respectiva legenda deve constar na página que contém a imagem.
 images_by_page=[sum(b.get('type')==1 for b in p.get_text('dict')['blocks']) for p in doc]
 for idx in range(1,7):
  caption_pages=[pi for pi,p in enumerate(doc) if f'Figura {idx} —' in p.get_text()]
  if len(caption_pages)!=1 or images_by_page[caption_pages[0]]<1:
   raise RuntimeError(f'Figura {idx} não está na mesma página de sua legenda: {caption_pages}')

 assert all(re.search(r'\bQ'+str(i)+r'\b',t) for i in range(1,15))
 assert all(re.search(r'\bS'+str(i)+r'\b',t) for i in range(1,5))
 assert all('[R'+str(i)+']' in t for i in range(1,22))
 assert all(re.search(r'\bE'+str(i)+r'\b',t) for i in range(1,10))
 assert 'páginas totais' in t and 'página' in t
 escaped=[]
 for pi,p in enumerate(doc,1):
  if not p.get_text().strip():raise RuntimeError(f'Página vazia {pi}')
  for b in p.get_text('blocks'):
   if len(b)>=7 and b[6]==0:
    x0,y0,x1,y1=b[:4]
    if x0<-1 or y0<-1 or x1>p.rect.width+1 or y1>p.rect.height+1:escaped.append((pi,(x0,y0,x1,y1)))
 if escaped:raise RuntimeError(f'Texto fora da página: {escaped[:5]}')
 d=Document(docx)
 assert len(d.inline_shapes)==6, len(d.inline_shapes)
 assert len(d.tables)==11, len(d.tables)
 assert all('[PÁGINA]' not in p.text for p in d.paragraphs)
 with zipfile.ZipFile(docx) as z:
  xml=z.read('word/document.xml').decode()
  if 'Table of Contents' in xml or '<w:sdt>' in xml:raise RuntimeError('Sumário Pandoc antigo ainda presente')
 print('AUDITORIA OK:',len(doc),'páginas PDF A4, anexos iniciam',pages[-2],'; 6 imagens, 11 tabelas; 18 questões, 21 refs')
 return pages

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--source',type=Path,default=Path('docs/relatorio_m2.md'))
 ap.add_argument('--outdir',type=Path,default=Path('dist'));ap.add_argument('--source-commit',default='')
 a=ap.parse_args();src=a.source.resolve();out=a.outdir.resolve();out.mkdir(parents=True,exist_ok=True)
 base=src.parent;figs=[base/'figuras_m2'/f'{i:02d}_{name}.svg' for i,name in [(1,'topologia'),(2,'operacao_kafka'),(3,'mecanismos'),(4,'e3_estatisticas'),(5,'particao'),(6,'recuperacao')]]
 for f in figs:
  if not f.exists():raise FileNotFoundError(f)
 text=src.read_text(encoding='utf-8')
 if 'A verificar no arquivo PDF final' in text:raise RuntimeError('Fonte contém pendência de validação antiga')
 with tempfile.TemporaryDirectory(prefix='m2_build_') as d:
  tmp=Path(d);fd=tmp/'figuras_m2';fd.mkdir()
  for svg in figs:
   run(['inkscape',svg,'--export-type=png','--export-filename='+str(fd/(svg.stem+'.png')),'--export-width=1900'])
  render_md=text.replace('figuras_m2/','figuras_m2/')
  render_md=re.sub(r'(figuras_m2/\d\d_[\w]+)\.svg',r'\1.png',render_md)
  # Preserve a SOURCE COPY, never overwrite Markdown during conversion.
  (tmp/'relatorio_render.md').write_text(render_md,encoding='utf-8')
  ref=tmp/'reference.docx';make_reference(ref)
  raw=tmp/'pandoc.docx'
  run(['pandoc',tmp/'relatorio_render.md','-f','markdown+yaml_metadata_block','-t','docx','--standalone','--toc','--toc-depth=2','--reference-doc='+str(ref),'-o',raw],cwd=tmp)
  initial=tmp/'intermediate.docx';build_docx(raw,initial,a.source_commit)
  pdf0=libreoffice_pdf(initial,tmp/'firstpdf');nums=locations(pdf0)
  fill_pages(initial,nums)
  target=out/'relatorio_m2.docx';shutil.copyfile(initial,target)
  final=libreoffice_pdf(target,out)
  # A correção do sumário NÃO pode alterar paginação; caso mude, falhar.
  actual=check_document(final,target,text)
  if actual!=nums:raise RuntimeError(f'Sumário não estável: {nums} versus {actual}')
  with fitz.open(final) as pd:
   meta=pd.metadata;meta.update({'title':'Trabalho M2 — Kafka KRaft × NATS JetStream',
    'subject':'Relatório M2 — UNIVALI; fonte docs/relatorio_m2.md',
    'keywords':'source_commit='+a.source_commit+'; source_sha256='+hashlib.sha256(src.read_bytes()).hexdigest()})
   pd.set_metadata(meta);pd.save(out/'relatorio_m2_tagged.pdf')
  (out/'relatorio_m2_tagged.pdf').replace(final)
  record={'source':'docs/relatorio_m2.md','source_commit':a.source_commit or 'não informado',
    'source_sha256':sha(src),'svg_sha256':{f.name:sha(f) for f in figs},
    'pdf_sha256':sha(final),'docx_sha256':sha(target),
    'pdf_pages':len(fitz.open(final)),'annex_start_page':actual[-2], 'annex_b_page':actual[-1],
    'renderer':'LibreOffice Writer (conversão DOCX→PDF)',
    'tocs':'sumário estático em português, números aferidos no PDF da mesma renderização',
    'note':'MS Word/versões/fontes diferentes podem repaginar o DOCX; atualizar sumário se houver reflow'}
  (out/'manifest.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
 print(json.dumps(record,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
