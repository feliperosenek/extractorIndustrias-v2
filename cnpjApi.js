const consultarCNPJ = require("consultar-cnpj");
const Sequelize = require("sequelize");
const { QueryTypes } = require("sequelize");
const dotenv = require("dotenv");
dotenv.config();

const token = process.env.TOKEN;

const sequelize = new Sequelize("eduard72_" + process.env.DATABASE + "", "eduard72_wp625", "37@S0DSm(p", {
  host: "sh-pro20.hostgator.com.br",
  dialect: "mysql",
  define: {
    freezeTableName: true,
    timestamps: false,
  },
  logging: false,
});

sequelize
  .authenticate()
  .then(() => {})
  .catch(err => {
    console.error("Erro ao conectar a banco de dados: ", err);
  });

async function getCNPJ() {
  var page = 0;

  try {
    // --> Pesquisa os CNAE's no banco
    var cnaes = await sequelize.query("SELECT id, cnae FROM "+process.env.TABLE_CNAE+" WHERE statusBot" + process.env.BOT + "=0", {
      type: QueryTypes.SELECT,
    });
    
    console.log("->> Extractor Industrias <<--")
    console.log("----------->> bot "+process.env.BOT+" <<------------")
    console.log()
    console.log("CNAE's disponíveis")
    console.log(cnaes)
    console.log()

    console.log("Iniciando pesquisa...........")
    console.log()

    // --> Abre um loop para pesquisar sobre os cnaes
    for (i = 0; i < cnaes.length; i++) {
      cnaesSearch = cnaes[i].cnae;

       data = await consultarCNPJ.pesquisa(
        {
          atividade_principal_id: cnaesSearch,
          estado_id: process.env.ESTADO_ID,
          situacao_cadastral: "Ativa"
        },
        token,
        1
      );
     
      console.log("cnae id: " + cnaes[i].id + " - CNAE: " + cnaesSearch + " - total: " + data.paginacao.total + "  |" + (i + 1) + "º de " + cnaes.length + " | Páginas: " + data.paginacao.paginas);
      console.log(" - - " + process.env.ACAO + " - - ");

      var pageRound = process.env.PAGINAS;

      if (data.paginacao.paginas < pageRound) {
        pageRound = data.paginacao.paginas;
      }

      const verPag = await sequelize.query("SELECT paginacaoBot" + process.env.BOT + " FROM `cnaes` WHERE cnae=" + cnaesSearch + "", {
        type: QueryTypes.SELECT,
      });

      var bot = "paginacaoBot"+process.env.BOT
      var somaPaginas = verPag[0][bot]
      somaPaginas = parseInt(somaPaginas)

      console.log("Paginação BD: "+somaPaginas)
      console.log()

      if(somaPaginas >= process.env.PAGINAS){
        pageRound=0
      }


      for (var t = 0; t < pageRound; t++) {

        const pagination = await sequelize.query("SELECT paginacaoBot" + process.env.BOT + " FROM `cnaes` WHERE cnae=" + cnaesSearch + "", {
          type: QueryTypes.SELECT,
        });
  
        var botPagination = "paginacaoBot"+process.env.BOT
        var paginasSoma = pagination[0][botPagination]
        paginasSoma = parseInt(paginasSoma)

        // --> Define uma página aleatória
        if (data.paginacao.paginas > process.env.PAGINAS) {
          page = Math.floor(Math.random() * data.paginacao.paginas) + 1;
        } else {
          page = t + 1;
        }

        console.log("Página: " + page + "  | " + (i + 1) + "º de " + cnaes.length+" | ( "+ (t+1)+" ) "+"Pagination  -> "+(paginasSoma+1));

        // --> Pesquisa por página

        if(paginasSoma >= process.env.PAGINAS){

        }else{

        const data2 = await consultarCNPJ.pesquisa(
          {
            // --> Consulta o CNAE
            atividade_principal_id: cnaesSearch,
            estado_id: process.env.ESTADO_ID
          },
          token,
          page
        );

        // --> Processamento da pesquisa no array industria
        for (x = 0; x < data2.data.length; x++) {
          const industria = await consultarCNPJ(data2.data[x], token); // --> Consulta o CNPJ

          nome = industria.razao_social;
          cnpj = industria.estabelecimento.cnpj;
          fantasia = industria.estabelecimento.nome_fantasia;
          endereco = industria.estabelecimento.tipo_logradouro + " " + industria.estabelecimento.logradouro;
          numero = industria.estabelecimento.numero;
          tipo_logradouro = industria.estabelecimento.tipo_logradouro;
          complemento = industria.estabelecimento.complemento;
          bairro = industria.estabelecimento.bairro;
          cep = industria.estabelecimento.cep;
          municipio = industria.estabelecimento.cidade.nome;
          uf = industria.estabelecimento.estado.sigla;
          pais = industria.estabelecimento.pais.nome;
          ddd_telefone = industria.estabelecimento.ddd1;
          telefone = industria.estabelecimento.telefone1;
          email = industria.estabelecimento.email;
          ddd_telefone2 = industria.estabelecimento.ddd2;
          telefone2 = industria.estabelecimento.telefone2;
          cnae = industria.estabelecimento.atividade_principal.id;
          cadastro = industria.estabelecimento.situacao_cadastral;
          if (industria.porte) {
            porte = industria.porte.descricao;
          } else {
            porte = "";
          }
          produto_2 = industria.estabelecimento.atividade_principal.descricao;
          produto_3 = industria.estabelecimento.atividades_secundarias;
          tipo = industria.estabelecimento.tipo;
          capital = industria.capital_social;
          produto_1 = "Industria";
          inscricao = "Regular";
          filial = 0;
          matriz = 0;
          fundacao = industria.estabelecimento.data_inicio_atividade;

          // filtros
          if (fundacao != null) {
            fundacao = fundacao.split("-");
            fundacao = fundacao[0];
          }

          var atividade = [];
          if (produto_3 !== null) {
            for (n = 0; n < produto_3.length; n++) {
              atividade.push(produto_3[n].descricao) + " ";
            }
          }

          produto_3 = atividade.join();

          if (tipo != null) {
            if (tipo.includes("Matriz")) {
              matriz = 1;
            }

            if (tipo.includes("Filial")) {
              filial = 1;
            }
          }

          const campos = ["nome", "cnpj", "fantasia", "endereco", "tipo_logradouro", "numero", "complemento", "bairro", "cep", "municipio", "uf", "pais", "ddd_telefone", "telefone", "email", "ddd_telefone2", "telefone2", "capital", "cnae", "produto_1", "produto_2", "produto_3", "matriz", "filial"];

          for (const variavel of campos) {
            if (!eval(variavel)) {
              eval(`${variavel} = ""`);
            }
          }

          var res = 0;

          // --> Verifica se o CNPJ já está cadastrado no banco de dadso
          const verificaDuplicata = await sequelize.query("SELECT cnpj FROM `catalogo` WHERE cnpj='" + cnpj + "'", {
            type: QueryTypes.SELECT,
          });

          // --> Filtros para inserir no banco de dados
          if (verificaDuplicata != "" || nome.includes("'") || endereco.includes("'") || bairro.includes("'") || municipio.includes("'") || fantasia.includes("'") || tipo_logradouro.includes("'"))  || cadastro != "Ativa") {
          } else {
            try {
             await sequelize.query("INSERT INTO catalogo (nome,cnpj, fantasia, endereco, tipo_logradouro, numero,complemento, bairro, cep, municipio, uf,pais,ddd_telefone, telefone, email, ddd_telefone2, telefone2, capital, cnae, produto_1, produto_2, produto_3, matriz, filial,ano_fundacao,produtos,materias_primas,nro_funcionarios,importa,exporta, porte, ativa) VALUES ('" + nome + "','" + cnpj + "','" + fantasia + "','" + endereco + "','" + tipo_logradouro + "','" + numero + "','" + complemento + "','" + bairro + "','" + cep + "','" + municipio + "','" + uf + "','" + pais + "','" + ddd_telefone + "','" + telefone + "','" + email + "','" + ddd_telefone2 + "','" + telefone2 + "','" + capital + "','" + cnae + "','" + produto_1 + "','" + produto_2 + "','" + produto_3 + "','" + matriz + "','" + filial + "','" + fundacao + "',0,0,0,0,0,'" + porte + "','" + cadastro + "' ) ");
              res = 1;
            } catch {
              console.log("ERRO MYSQL <-------------");
            }
          }
          if (res == 0) {
            res = "";
          } else {
            res = "    ✔ bd ";
          }

          console.log("  ✔  " + nome + res);
         
        }
      }

        const paginacaoBD = await sequelize.query("SELECT paginacaoBot" + process.env.BOT + " FROM `cnaes` WHERE cnae=" + cnaesSearch + "", {
          type: QueryTypes.SELECT,
        });

        var pag = "paginacaoBot"+process.env.BOT

        somaPag = paginacaoBD[0][pag]
        somaPag = parseInt(somaPag)
        somaPag = somaPag+1
      

        await sequelize.query("UPDATE "+process.env.TABLE_CNAE+" SET paginacaoBot" + process.env.BOT + "="+somaPag+" WHERE cnae=" + cnaesSearch + "");


        pages = await data.paginacao.paginas;
      }
      // --> Informa que o cnae já foi usado
      await sequelize.query("UPDATE "+process.env.TABLE_CNAE+" SET statusBot" + process.env.BOT + "=1 WHERE cnae=" + cnaesSearch + "");

    }
  } catch (error) {
    console.log(error);
    process.exit(1);
  }
}

getCNPJ();
