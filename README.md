# Web Scraping de Imóveis


## Conteúdo

- [Descrição do Projeto](#descrição-do-projeto)
- [Estrutura](#estrutura)
- [Problema inicial e solução](#problema-inicial-e-solução)

## Descrição do Projeto

Este projeto visa criar uma estrutura de código capaz de realizar uma raspagem de dados na base do Zapimoveis passando parâmetros como local do imóvel e tipo do imóvel (casa, apartamento, etc), e retorando os dados disponíveis na página do site, como valor de aluguel, comodidades e localização. Tudo isso em um app do streamlit, com algumas visualizações, uma interface gráfica do Pygwalker para que o usuário posso manipular os dados e criar visualizações e por fim duas opções de download das bases inteiras, tanto em csv quanto em xlsx.

## Problema inicial e solução
Inicialmente, como forma de otimizar a coleta de dados, a ideia era utilizar a biblioteca requests e assim coletar os dados através de comandos GET. No entanto, o conteúdo da site da Zapimoveis é dinâmico, o que implica na necessidade de o usuário interagir com o site para que todos os imóveis de cada página aparecem. Em resumo, é necessário rolar a página até que todos os imóveis, aproximadamente 100 por página, apareçam.

Nesse novo cenário, não consegui encontrar possibilidades de utilizar bibliotecas como a requests e a solução foi utilizar selenium, uma vez que este possibilita controlar o navegador e realizar a rolagem da página. Apesar de o selenium ter facilitado a obtenção dos dados e até a solução de problemas como limite de requisições, isso veio a um custo maior de performance e um problema futuro que não consegui resolver, que foi a dificuldade de fazer o app funcionar no Streamlit Cloud. Quanto ao primeiro problema, a saída que encontrei para melhorar um pouco a performance foi utilizar a biblioteca concurrent.futures para realizar múltiplas tarefas de forma concorrente.

## Estrutura
- Utils
    - utils_scraper:
    - utils_views

## Link do app

