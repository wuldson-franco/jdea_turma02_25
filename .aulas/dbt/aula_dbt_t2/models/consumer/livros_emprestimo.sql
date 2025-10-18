{{ config(materialized='table') }}

with emprestimos_por_livro as (
    select 
        l.livro_id, 
        l.titulo,
        count(e.emprestimo_id) as total_emprestimos
    from {{ ref('livros') }} l
    join {{ ref('emprestimos') }} e on l.livro_id = e.livro_id
    group by l.livro_id, l.titulo
)
select *
from emprestimos_por_livro
order by total_emprestimos desc
limit 1;