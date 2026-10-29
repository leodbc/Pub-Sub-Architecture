# Reconstrução do Relatório M2 (Fase 3)

Fonte autoral única: `docs/relatorio_m2.md`; figuras editáveis: `docs/figuras_m2/01_*.svg` a `06_*.svg`. O script `scripts/build_m2_report.py` constrói DOCX, converte **esse mesmo DOCX** em PDF e registra os hashes dos insumos e saídas, a paginação e o commit informado em `dist/manifest.json`.

## Requisitos locais

- Linux, Python 3 com `python-docx` e `PyMuPDF` (com versões verificadas por `python -m pip freeze`).
- `pandoc`, `inkscape` e `libreoffice` disponíveis no `PATH`.
- Para paginação comparável: **Liberation Serif**, **Liberation Sans**, configuração A4 e versões de ferramentas/fontes compatíveis. Diferentes sistemas de fontes, LibreOffice ou Microsoft Word podem mudar quebras de página, mesmo com texto idêntico.
- Não requer Kafka, NATS, Docker, GitHub Actions nem baixar os ZIPs experimentais para renderizar; os resultados estão congelados no Markdown e são rastreados ao manifesto da Fase 1.

Na raiz do repositório:

```bash
python -m pip install -r scripts/requirements-relatorio.txt
python scripts/build_m2_report.py --source docs/relatorio_m2.md --outdir dist --source-commit "$(git rev-parse HEAD)"
```

O script falha se as âncoras do sumário mudarem inesperadamente, se faltarem figuras/tabelas/questões/referências, se restar `Table of Contents`, se a paginação esperada for alterada ou se houver blocos textuais fora do papel A4. **Não ajusta o conteúdo científico nem dispara experimentos**.

## Reproduzível versus externo

Reproduzíveis com os requisitos acima: a estrutura, texto, tabelas, seis SVGs, paginação via LibreOffice, sumário e arquivos finais em `dist/`. O desenho visual foi estabilizado para o motor LibreOffice utilizado; diferenças nos binários bit a bit são possíveis por metadados, fontes e versões.

Dependem de serviços/ambientes externos: a preservação dos artefatos históricos GitHub, o próprio GitHub Actions e eventuais reexecuções E1–E9, as imagens Docker e a instrumentação física do runner. O DOCX é editável e **não possui um sumário automático Word**: há um sumário estático com páginas verificadas no PDF produzido; se for editado/reaberto em Microsoft Word e repaginado, o editor deverá atualizá-lo. Em particular, o script não pode prometer a mesma paginação em todas as instalações do Microsoft Word.

## Proveniência e auditoria humana

`dist/manifest.json` grava o commit do **documento-fonte** passado no comando, o SHA-256 desse Markdown e dos seis SVG, e os SHA-256 dos dois binários. Compare `source_sha256` com os bytes do arquivo no commit indicado. A auditoria de cortes e páginas do script é técnica e não substitui a inspeção visual do PDF e DOCX e a revisão acadêmica humana. Não preencher automaticamente nomes, matrículas, turma, contribuições individuais ou confirmação humana de IA.

**Versões da renderização validada em 28/10/2026:** Pandoc 3.1.11.1; LibreOffice 25.2.3.2; Inkscape 1.4; Python `python-docx` 1.2.0 e PyMuPDF 1.26.7. As dependências Python estão restritas por faixas no arquivo `requirements-relatorio.txt`; para reprodutibilidade visual estrita, registre também as versões do sistema e fontes.
