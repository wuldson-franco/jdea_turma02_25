-- Use the `ref` function to select from other models

select id,
       nome as nome_pessoa,
       cidade as cidade_pessoa,
       estado as estado_pessoa
from "postgres"."public"."pessoa"
where id = 1