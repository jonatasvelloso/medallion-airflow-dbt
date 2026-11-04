-- Executado pelo container postgres na primeira inicialização.
-- Em produção cada origem seria um servidor diferente; aqui elas convivem
-- em um único Postgres para o ambiente subir em qualquer notebook.

CREATE DATABASE airflow;   -- metadados do Airflow
CREATE DATABASE alfa;      -- sistema de origem da Alfa Fibra
CREATE DATABASE beta;      -- sistema de origem da Beta Net
CREATE DATABASE gama;      -- sistema legado da Gama Telecom
CREATE DATABASE dw;        -- Data Warehouse (bronze, staging, silver, gold)
