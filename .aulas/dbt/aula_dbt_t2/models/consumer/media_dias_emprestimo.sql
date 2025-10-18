{{ config(materialized='table') }}

with emprestimos_finalizados as (
    select 
        emprestimo_id,
        data_emprestimo,
        data_devolucao,
        (cast(data_devolucao as date) - cast(data_emprestimo as date)) as dias_emprestimo
    from {{ ref('emprestimos') }}
    where status = 'finalizado'
)
select avg(dias_emprestimo) as media_dias_emprestimo
from emprestimos_finalizados;
