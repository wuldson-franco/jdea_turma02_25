
  create view "postgres"."public"."my_second_dbt_model__dbt_tmp"
    
    
  as (
    -- Use the `ref` function to select from other models

select id,
       nome as nome_pessoa,
       cidade as cidade_pessoa,
       estado as estado_pessoa
from "postgres"."public"."pessoa"
where id = 1
  );