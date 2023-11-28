# Web Scraping de Imóveis


> ## Conteúdo

- [Descrição do Projeto](#descrição-do-projeto)
- [Problema inicial e solução](#problema-inicial-e-solução)
- [Estrutura](#estrutura)
    - [Dados geográficos](#dados-geográficos)
    - [Utils](#utils)
- [Link do App](#link-do-app)
- [Conclusão](#conclusão)

> ## Descrição do Projeto

Este projeto visa criar uma estrutura de código capaz de realizar uma raspagem de dados na base do Zapimoveis passando parâmetros como local do imóvel e tipo do imóvel (casa, apartamento, etc), e retornando os dados disponíveis na página do site, como valor de aluguel, comodidades e localização. Tudo isso em um app do streamlit, com algumas visualizações, uma interface gráfica do Pygwalker para que o usuário posso manipular os dados e criar visualizações e por fim duas opções de download das bases inteiras, tanto em csv quanto em xlsx.

> ## Problema inicial e solução
Inicialmente, como forma de otimizar a coleta de dados, a ideia era utilizar a biblioteca requests e assim coletar os dados através de comandos GET. No entanto, o conteúdo do site da Zapimoveis é dinâmico, o que implica na necessidade de o usuário interagir com o site para que todos os imóveis de cada página apareçam. Em resumo, é necessário rolar a página até que todos os imóveis, aproximadamente 100 por página, apareçam.

Nesse novo cenário, não consegui encontrar possibilidades de utilizar bibliotecas como a requests e a solução foi utilizar selenium, uma vez que este possibilita controlar o navegador e realizar a rolagem da página. Apesar de o selenium ter facilitado a obtenção dos dados e até a solução de problemas como limite de requisições, isso veio a um custo maior de performance e um problema futuro que não consegui resolver, que foi a dificuldade de fazer o app funcionar no Streamlit Cloud. Quanto ao primeiro problema, a saída que encontrei para melhorar um pouco a performance foi utilizar a biblioteca concurrent.futures para realizar múltiplas tarefas de forma concorrente.

> ## Estrutura
- ### Dados geográficos:
    - Dados de todas as cidades do Brasil foram obtidos através da API do IBGE utilizando requests e posteriormente salvas em formato parquet para consumo no filtro do app.
- ### Utils
    - **utils_scraper**:
        - Classe com os métodos usados para realizar a raspagem dos dados
        - *paginas(self)*: Recebe como parâmetros os valores passados no constructor, que são o tipo de imóvel e o local, e retorna uma estimativa do total de páginas, considerando que cada página tem aproximadamente 100 imóveis
        - *scraping(self, workers: int = 3)*: Com base no número de paginas retornado no método paginas, realiza a obtenção do html das páginas usando Selenium para realizar a rolagem até o final. Como utiliza a biblioteca concurrent.futures, recebe como um dos parâmetros o número máximo de workers para realizar as tarefas de forma concorrente.
        - *tratamento_scraping(self, db_name: str = 'scraping', table_name: str = 'dados_imoveis_raw', if_exists: str = 'append')*: Com a lista de html gerados no método scraping, esse método realiza o tratamento usando Beautifulsoup e retorna um dataframe com os dados. Além disso, salva os dados no formato parquet particionado por ano, mes e dia de extração.
        - *scraping_multiple(self, _transacao*: list = ['aluguel'], _tipo: list = ['apartamentos'], _local: list = ['se+aracaju'], workers: int = 2): Método com o objetivo de juntar os métodos anteriores e realizar a raspagem com base em múltiplos valores de local e tipo.
    - **utils_views**:
        - Classe com métodos para checar se os dados solicitados já existem na base criada em parquet e, com isso, retorna os dados selecionados, além de gerar alguns cards do Streamlit.
        - *check_base(self)*: Avalia a requisição no app e retorna um dataframe baseado nos resultados, i.e, caso os dados existam, retorna a base existente, caso não, realiza a raspagem dos dados dos tipos faltantes.
        - *base_agg(self)*: Retorna a base de imóveis agrupada com dados de aluguel e amenidades por bairro. Além disso, trata os dados conforme cálculo dos limites superiores e inferiores dos valores de aluguel.
        - *st_cards(self)*: Retorna cards no streamlit de totalidade dos imóveis, média de aluguel e tipo de imóvel mais comum com base no local e tipo selecionado.

> ## Link do app
- Link: https://scrapingzapimoveis-jnga.streamlit.app/

> ## Conclusão
Este projeto foi extremamente importante para melhorar meus conhecimentos em web scraping, git, consumo de API's e manipulação de dados. Considero que, apesar de não ter conseguido alcançar o objetivo de disponibilizar o app livre para qualquer um consultar cidades específicas, atingi o objetivo de realizar coleta em massa de dados de um site complexo como o da Zapimoveis, por seu contexto dinâmico.

Fico aberto a todas as críticas construtivas e dicas de como melhorar o projeto ou contornar os problemas citados. Caso queira entrar em contato, segue meu Linkedin: https://www.linkedin.com/in/niltonandrade/

Muito obrigado!

