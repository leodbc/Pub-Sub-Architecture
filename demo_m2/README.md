# Demonstração do seminário via GitHub Actions

Executa a sequência operacional Kafka KRaft + NATS JetStream em um runner Ubuntu descartável, derivado do commit auditado 24a98f9 do PR #5.

**Não altera os experimentos históricos nem repete benchmarks.** Mostra E1 (topologia), E2 (publicação/leitura), E4 (ordem), E3 (uma leitura didática por follower, classificada separadamente), E5/E7 (falha e recuperação) e E9 (JetStream). Todas as saídas permanecem no artifact do workflow, com logs inclusive em caso de falha. A leitura E3 didática não é apresentada como as cinco observações históricas.

A abertura do PR dispara automaticamente o workflow de pull_request por filtro de arquivos. O job não exige computador local, não altera main e não faz merge.

**Limite acadêmico:** GitHub Actions permite demonstrar uma execução real e inspecionar logs, mas seus runners são efêmeros. Isso não garante, por si só, a exigência de cluster ativo no início e oito minutos de demonstração presencial ao vivo. A página 42 sugere quatro minutos de demonstração, em conflito com o mínimo de oito minutos da página 41. A interpretação requer o professor.

Verificação de segurança: o script exige que 24a98f9 seja ancestral, confere a identidade dos arquivos científicos por hashes Git, escolhe o líder dinamicamente, verifica o rótulo do projeto Docker antes de pará-lo e o restaura mesmo diante de falha. Não faz reset destrutivo, não faz merge nem altera o código científico.
