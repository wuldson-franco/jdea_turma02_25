

with pessoa as (

    select 1 as id,
           'Wuldson' as nome,
           'Mataraca' as cidade,
           'Paraiba' as estado
    --union all
    --select null as id, nome, cidade, estado
)
select *
from pessoa

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null