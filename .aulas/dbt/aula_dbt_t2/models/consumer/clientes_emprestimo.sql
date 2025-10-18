{{ config(materialized='table') }}

with emprestimos_por_cliente as (
    select 
        c.cliente_id,
        c.nome,
        c.sobrenome,
        count(e.emprestimo_id) as total_emprestimos
    from {{ ref('clientes') }} c
    join {{ ref('emprestimos') }} e on c.cliente_id = e.cliente_id
    group by c.cliente_id, c.nome, c.sobrenome
)
select *
from emprestimos_por_cliente
order by total_emprestimos desc
limit 1;
