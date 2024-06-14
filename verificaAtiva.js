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
  .then(() => { })
  .catch(err => {
    console.error("Erro ao conectar a banco de dados: ", err);
  });

async function getCNPJ() {

  try{
  const industrias = await sequelize.query("SELECT cnpj FROM `catalogo` WHERE google=1 && atualizado=2 ORDER BY rand()", {
    type: QueryTypes.SELECT,
  });

    console.log("Total de Industrias: "+industrias.length)

  for (const ind of industrias) {
    const industria = await consultarCNPJ(ind.cnpj, token);  
     cnpj = industria.estabelecimento.cnpj;
    cadastro = industria.estabelecimento.situacao_cadastral;
    nome = industria.razao_social;

    if (cadastro == "Ativa") {
      console.log(nome+" - "+cadastro)
      try {
        await sequelize.query("UPDATE `catalogo` SET atualizado=3 WHERE cnpj='" + cnpj + "'");
        await sequelize.query("UPDATE `catalogo` SET ativa='sim' WHERE cnpj='" + cnpj + "'");
        res = 1;
      } catch (error){        
        console.log("ERRO MYSQL <-------------");
        console.log(error)
      }
    } else {
      console.log(nome+" - "+cadastro)
    }

  }

  await new Promise(resolve => setTimeout(resolve, 1000));
  }catch(error){
    console.log(error)
  }

}
getCNPJ();
