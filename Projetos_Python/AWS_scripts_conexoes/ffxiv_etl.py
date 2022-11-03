# %% [markdown]
# Bibliotecas utilizadas

# %%
import time
import pandas as pd
import requests as req
import datetime as date
import numpy as np
import boto3
from io import StringIO
from io import BytesIO

# %% [markdown]
# Funções salvas

# %%
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946614930?data=FCM")
    return response.json()

def discord():
    headers = {
        'authorization':'MTgxMTQxOTc4NDI2MjQ1MTQx.GWyYGY.tmzxSxYRbfvoS5YlfVl-cfY8vOia3LqiOBYsh8'
    }
    response = req.get(f"https://discord.com/api/v9/channels/766131659111268392/messages", headers=headers)
    return response.json()

def discord_50_m(id):
    headers = {
        'authorization':'MTgxMTQxOTc4NDI2MjQ1MTQx.GWyYGY.tmzxSxYRbfvoS5YlfVl-cfY8vOia3LqiOBYsh8'
    }
    response = req.get(f"https://discord.com/api/v9/channels/766131659111268392/messages?before=" + id + "&limit=50", headers=headers)
    return response.json()
    
def upload_s3(file,paste,bucket,df):
    s3_file_key = str(paste)+"/"+str(file)
    s3 = boto3.client("s3",aws_access_key_id="%%%%%%%%%%", aws_secret_access_key="%%%%%%%%%%%")
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index = False)
    csv_buf.seek(0)
    s3.put_object(Bucket=bucket,Body=csv_buf.getvalue(),Key=s3_file_key)

# %%
print("Gerando tabela de membros do clan.")

# %%
D_MEMBROS_FC = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
D_MEMBROS_FC.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)

D_MEMBROS_FC["ID"] = D_MEMBROS_FC["ID"].astype(str) 
D_MEMBROS_FC["Lodestone"] = "https://na.finalfantasyxiv.com/lodestone/character/" + D_MEMBROS_FC["ID"] 


# %%
print("Tabela de membros gerada com sucesso.")
upload_s3("d_membros_fc.csv","client","aws-ffbucket",D_MEMBROS_FC)

# %%
jobs_dict ={
                "alchemist / alchemist":"N/A",            
                "arcanist / scholar":"HLR",               
                "arcanist / summoner":"DPS",              
                "archer / bard":"DPS",   
                "armorer / armorer":"N/A",
                "astrologian / astrologian":"HLR",        
                "blacksmith / blacksmith":"N/A",          
                "blue mage / blue mage":"DPS",            
                "botanist / botanist":"N/A",              
                "carpenter / carpenter":"N/A",            
                "conjurer / white mage":"HLR",            
                "culinarian / culinarian":"N/A",          
                "dancer / dancer":"DPS", 
                "dark knight / dark knight":"TNK",        
                "fisher / fisher":"N/A", 
                "gladiator / paladin":"TNK",              
                "goldsmith / goldsmith":"N/A",            
                "gunbreaker / gunbreaker":"TNK",          
                "lancer / dragoon":"DPS",
                "leatherworker / leatherworker":"N/A",    
                "machinist / machinist":"DPS",            
                "marauder / warrior":"TNK",               
                "miner / miner":"N/A",   
                "pugilist / monk":"DPS",
                "reaper / reaper":"DPS", 
                "red mage / red mage":"DPS",              
                "rogue / ninja":"DPS",   
                "sage / sage":"HLR",     
                "samurai / samurai":"DPS",
                "thaumaturge / black mage":"DPS",       
                "weaver / weaver":"HLR"      
    
    
            }

# %%
print("Gerando tabela BRUTA de Classes e Jobs.")

# %%
RAW_ClassJobs = pd.DataFrame([])
x=0
for x in range(0, D_MEMBROS_FC["ID"].count()):
    response = req.get("https://xivapi.com/character/"+ str(D_MEMBROS_FC["ID"][x]))
    sup= response.json()["Character"]["ClassJobs"]
    sup = pd.DataFrame(sup)
    sup.drop(list(sup.filter(regex = "Exp")), axis = 1, inplace = True)
    sup.drop(["ClassID","IsSpecialised","JobID","UnlockedState"], axis =1, inplace = True)
    sup["Lodestone"] = "https://na.finalfantasyxiv.com/lodestone/character/" + D_MEMBROS_FC["ID"][x]
    sup["ID"] = D_MEMBROS_FC["ID"][x]
    time.sleep(1.5)
    RAW_ClassJobs = pd.concat([RAW_ClassJobs,sup])

RAW_ClassJobs.reset_index(drop = True, inplace = True)
RAW_ClassJobs = RAW_ClassJobs.drop_duplicates()
D_MEMBROS_FC["ID"] = D_MEMBROS_FC["ID"].astype(float) 

# %%
print("Tabela Bruta de Classes e Jobs gerada com sucesso.")
upload_s3("raw_classjobs.csv","client","aws-ffbucket",RAW_ClassJobs)

# %%
ANALYTICS_90ClassJobs = RAW_ClassJobs

ANALYTICS_90ClassJobs = ANALYTICS_90ClassJobs.replace({"Name":jobs_dict})
ANALYTICS_90ClassJobs["Name"].replace(regex="N/A",value=float("NaN"), inplace= True)
ANALYTICS_90ClassJobs.dropna(subset="Name",inplace= True)

ANALYTICS_90ClassJobs = ANALYTICS_90ClassJobs.query('Level == 90')

sup = list(ANALYTICS_90ClassJobs.columns)
sup[sup.index("Name")] = "Role_Jobs"
ANALYTICS_90ClassJobs.columns = sup

ANALYTICS_90ClassJobs["Qtd_Jobs"] = ANALYTICS_90ClassJobs["Role_Jobs"]
ANALYTICS_90ClassJobs = ANALYTICS_90ClassJobs.groupby(["ID","Lodestone","Role_Jobs"])["Qtd_Jobs"].count().reset_index()
ANALYTICS_90ClassJobs = ANALYTICS_90ClassJobs.groupby(["ID","Lodestone"]).agg({"Role_Jobs":"count","Qtd_Jobs":"sum"}).reset_index()
ANALYTICS_90ClassJobs["ID"] = ANALYTICS_90ClassJobs["ID"].astype(float)

# %%
print("Tabela ANALITICA de classes no level 90 gerada com sucesso.")
upload_s3("analytics_90classjobs.csv","client","aws-ffbucket",ANALYTICS_90ClassJobs)

# %%
print("Gerando Tabela BRUTA de historico de entrada e saida dos membros.")

# %%
RAW_discord=pd.DataFrame(discord())

x = RAW_discord["id"].min() 

while x != "766849288331722803":
    sup = pd.DataFrame(discord_50_m(x))  
    x = sup["id"].min()
    RAW_discord = pd.concat([RAW_discord,sup])

RAW_discord.drop(["type","channel_id","author","attachments","embeds","mentions","mention_roles","pinned","mention_everyone","tts","edited_timestamp","flags","components"], axis =1 , inplace= True)    
RAW_discord["timestamp"] = RAW_discord["timestamp"].astype(str).str[:10]
sup = list(RAW_discord.columns)
sup[sup.index("content")] = "mensagem"
sup[sup.index("timestamp")] = "data"
RAW_discord.columns = sup
RAW_discord.reset_index(drop = True, inplace= True)
RAW_discord = RAW_discord.drop_duplicates()

# %%
print("Tabela BRUTA de historico dos membros gerada com sucesso")
upload_s3("raw_discord.csv","client","aws-ffbucket",RAW_discord)

# %%
print("Construindo tabela FATO.....")

# %%

ANALYTICS_discord_part1 = RAW_discord.loc[RAW_discord["mensagem"].str.contains('. Id:', regex=False)]
ANALYTICS_discord_part1 = ANALYTICS_discord_part1.join(ANALYTICS_discord_part1["mensagem"].str.split("(", expand = True))

sup = list(ANALYTICS_discord_part1.columns)
sup[sup.index(0)] = "mensagem_1"
sup[sup.index(1)] = "mensagem_2"
ANALYTICS_discord_part1.columns = sup
ANALYTICS_discord_part1.drop("mensagem", axis =1 , inplace = True)

ANALYTICS_discord_part1 = ANALYTICS_discord_part1.join(ANALYTICS_discord_part1["mensagem_2"].str.split(")", expand = True))

sup = list(ANALYTICS_discord_part1.columns)
sup[sup.index(0)] = "mensagem_3"
sup[sup.index(1)] = "mensagem_4"
ANALYTICS_discord_part1.columns = sup
ANALYTICS_discord_part1.drop(["mensagem_2","mensagem_3"], axis =1 , inplace = True)

ANALYTICS_discord_part1 = ANALYTICS_discord_part1.join(ANALYTICS_discord_part1["mensagem_1"].str.rsplit(" ", n=1, expand = True))

sup = list(ANALYTICS_discord_part1.columns)
sup[sup.index(0)] = "Name"
sup[sup.index(1)] = "mensagem_6"
ANALYTICS_discord_part1.columns = sup
ANALYTICS_discord_part1.drop(["mensagem_1","mensagem_6"], axis =1 , inplace = True)

ANALYTICS_discord_part1 = ANALYTICS_discord_part1.join(ANALYTICS_discord_part1["mensagem_4"].str.rsplit(" ", n=1, expand = True))
sup = list(ANALYTICS_discord_part1.columns)
sup[sup.index(0)] = "Status_entrada_saida"
sup[sup.index(1)] = "id_lodstone"
ANALYTICS_discord_part1.columns = sup
ANALYTICS_discord_part1.drop("mensagem_4", axis =1 , inplace = True)
ANALYTICS_discord_part1["Status_entrada_saida"].replace(regex = "da fc.", value = "", inplace = True)
ANALYTICS_discord_part1["Status_entrada_saida"].replace(regex = "na fc.", value = "", inplace = True)
ANALYTICS_discord_part1["Status_entrada_saida"].replace(regex = " ", value = "", inplace = True)
ANALYTICS_discord_part1["Status_entrada_saida"].replace(regex = "Id:", value = "", inplace = True)



# %%

ANALYTICS_discord_part2 = RAW_discord.loc[RAW_discord["mensagem"].str.contains('. Id:', regex=False)==False]
ANALYTICS_discord_part2 = ANALYTICS_discord_part2.join(ANALYTICS_discord_part2["mensagem"].str.split("(", expand = True))

sup = list(ANALYTICS_discord_part2.columns)
sup[sup.index(0)] = "mensagem_1"
sup[sup.index(1)] = "mensagem_2"
ANALYTICS_discord_part2.columns = sup
ANALYTICS_discord_part2.drop("mensagem", axis =1 , inplace = True)

ANALYTICS_discord_part2 = ANALYTICS_discord_part2.join(ANALYTICS_discord_part2["mensagem_2"].str.split(")", expand = True))

sup = list(ANALYTICS_discord_part2.columns)
sup[sup.index(0)] = "mensagem_3"
sup[sup.index(1)] = "Status_entrada_saida"
ANALYTICS_discord_part2.columns = sup
ANALYTICS_discord_part2.drop(["mensagem_2","mensagem_3"], axis =1 , inplace = True)

ANALYTICS_discord_part2 = ANALYTICS_discord_part2.join(ANALYTICS_discord_part2["mensagem_1"].str.rsplit(" ", n=1, expand = True))

sup = list(ANALYTICS_discord_part2.columns)
sup[sup.index(0)] = "Name"
sup[sup.index(1)] = "mensagem_6"
ANALYTICS_discord_part2.columns = sup
ANALYTICS_discord_part2.drop(["mensagem_1","mensagem_6"], axis =1 , inplace = True)


ANALYTICS_discord_part2["Status_entrada_saida"].replace(regex = "da fc.", value = "", inplace = True)
ANALYTICS_discord_part2["Status_entrada_saida"].replace(regex = "na fc.", value = "", inplace = True)
ANALYTICS_discord_part2["Status_entrada_saida"].replace(regex = " ", value = "", inplace = True)
ANALYTICS_discord_part2["id_lodstone"]=" "


# %%
ANALYTICS_discord = pd.concat([ANALYTICS_discord_part1,ANALYTICS_discord_part2])
ANALYTICS_discord = ANALYTICS_discord.sort_values(by="id")

# %%
ANALYTICS = ANALYTICS_discord
ANALYTICS["Entrou_saiu_2chave"] = float("NaN")
ANALYTICS["Entrou_saiu_3chave"] = float("NaN")
for x in list(ANALYTICS["Name"].unique()):
    sup = 1
    for y in list(ANALYTICS.loc[ANALYTICS["Name"]== x].reset_index()["index"]):
        if ANALYTICS["Status_entrada_saida"][y]== "entrou":
            sup+=1
        ANALYTICS["Entrou_saiu_2chave"][y]= sup

    for y in range(0, ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"].count()):
        try:
           if (ANALYTICS["Status_entrada_saida"][list(ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"])[y]] + ANALYTICS["Status_entrada_saida"][list(ANALYTICS.loc[ANALYTICS["Name"]== x].reset_index()["index"])[y-1]] + ANALYTICS["Status_entrada_saida"][list(ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"])[y+1]]) == "saiusaiuentrou":
               ANALYTICS["Entrou_saiu_3chave"][list(ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"])[y]] = "nok"
           else:
               ANALYTICS["Entrou_saiu_3chave"][list(ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"])[y]] = "ok"
        except:
           ANALYTICS["Entrou_saiu_3chave"][list(ANALYTICS.loc[ANALYTICS["Name"]== x ].reset_index()["index"])[y]] = "ok"

# %%
join_de_para = pd.DataFrame([])
join_de_para["id"] = ANALYTICS_discord_part1["id_lodstone"].unique()
join_de_para["Name"] = ANALYTICS_discord_part1["Name"].unique()


# %%
ANALYTICS = ANALYTICS.pivot(index=["Name","Entrou_saiu_2chave","Entrou_saiu_3chave"], columns="Status_entrada_saida",values="data").reset_index()


# %%
ANALYTICS = ANALYTICS.merge(join_de_para, how = "left", on= "Name")

ANALYTICS.drop(["Entrou_saiu_2chave","Entrou_saiu_3chave"],axis= 1, inplace=True)
sup = list(ANALYTICS.columns)
sup[sup.index("entrou")] = "Data_entrada"
sup[sup.index("saiu")] = "Data_saida"
ANALYTICS.columns = sup

# %%
ANALYTICS = ANALYTICS.merge(D_MEMBROS_FC, how= "left",on="Name")
ANALYTICS["id"] = ANALYTICS["id"].astype(float)
ANALYTICS = pd.merge(ANALYTICS,D_MEMBROS_FC, how= "left",left_on="id",right_on="ID")


# %%
ANALYTICS["ID"] = float("NaN")
ANALYTICS["ID_x"] = ANALYTICS["ID_x"].astype(float)
ANALYTICS["ID_y"] = ANALYTICS["ID_y"].astype(float)
for x in range(0,ANALYTICS["Name_x"].count()):
    if (np.isnan(ANALYTICS["ID_y"][x])==True):
        ANALYTICS["ID"][x] = ANALYTICS["ID_x"][x]
    else:
        ANALYTICS["ID"][x] = ANALYTICS["ID_y"][x]

# %%
Status_presente_presente = np.logical_and(np.logical_and(pd.isnull(ANALYTICS["ID"])==False,pd.isnull(ANALYTICS["Data_entrada"])==False),pd.isnull(ANALYTICS["Data_saida"])==True)
Status_presente_ausente = np.logical_and(pd.isnull(ANALYTICS["Data_entrada"])==False,pd.isnull(ANALYTICS["Data_saida"])==False)

# %%
ANALYTICS.drop(["id","Avatar_x","Avatar_y","ID_x","Rank_x","Lodestone_x","ID_y","Name_y","Rank_y","Lodestone_y"],axis=1, inplace=True)
upload_s3("fato_historico_membros.csv","client","aws-ffbucket",ANALYTICS)
print("Tabela FATO gerada com sucesso.")

# %%
ANALYTICS["Status_presente_fc"] = float("NaN")
ANALYTICS["Status_presente_fc"].loc[Status_presente_presente] = "Presente"
ANALYTICS["Status_presente_fc"].loc[Status_presente_ausente] = "Ausente"
ANALYTICS["Status_presente_fc"].loc[ANALYTICS["Status_presente_fc"].isnull()] = "Data inconsistente"

sup = list(ANALYTICS.columns)
sup[sup.index("Name_x")] = "Name"
ANALYTICS.columns = sup

# %%
ANALYTICS["Qtd_dias"] = float("NaN")
x=0
sup = pd.Series([], dtype="object")
for x in range(0, ANALYTICS["Name"].count()):
    if ANALYTICS["Status_presente_fc"][x] =="Presente":
        sup[x] = (date.datetime.today() - date.datetime.strptime(ANALYTICS["Data_entrada"][x],'%Y-%m-%d')).days
    elif ANALYTICS["Status_presente_fc"][x] =="Ausente":
        sup[x] = (date.datetime.strptime(ANALYTICS["Data_saida"][x],'%Y-%m-%d') - date.datetime.strptime(ANALYTICS["Data_entrada"][x],'%Y-%m-%d')).days
    elif ANALYTICS["Status_presente_fc"][x] =="Data inconsistente":
        sup[x] = 0
       
ANALYTICS["Qtd_dias"] = sup


# %%
ANALYTICS = ANALYTICS.merge(ANALYTICS_90ClassJobs, how= "left", on= "ID")
ANALYTICS.drop(["Lodestone"], axis =1 , inplace=True)

ANALYTICS = ANALYTICS.merge(D_MEMBROS_FC, how= "left", on= "ID")
ANALYTICS.drop(["Avatar","Name_y","Lodestone"], axis =1 , inplace=True)

# %%
filtro_cargos_sapling = np.logical_and(ANALYTICS["Qtd_dias"]<90, ANALYTICS["Status_presente_fc"]=="Presente")
filtro_cargos_senior_tree = np.logical_and(np.logical_and(np.logical_and(ANALYTICS["Qtd_dias"]>=90, ANALYTICS["Qtd_dias"]<180), ANALYTICS["Role_Jobs"]>0),ANALYTICS["Status_presente_fc"]=="Presente")
filtro_cargos_lunar_tree = np.logical_and(np.logical_and(ANALYTICS["Qtd_dias"]>=180, ANALYTICS["Role_Jobs"]==3),ANALYTICS["Status_presente_fc"]=="Presente")

# %%
ANALYTICS["Rank_recomendado"] = float("NaN")
ANALYTICS["Rank_recomendado"].loc[filtro_cargos_sapling==True] ="Sapling"
ANALYTICS["Rank_recomendado"].loc[filtro_cargos_senior_tree==True] ="Senior Tree"
ANALYTICS["Rank_recomendado"].loc[filtro_cargos_lunar_tree==True] ="Lunar Tree"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank_recomendado"].isnull()] = ANALYTICS["Rank"]
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank"]=="Lunar Tree"] = "Lunar Tree"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank"]=="1st Div - Mod"] = "1st Div - Mod"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank"]=="2nd Div - Tech"] = "2nd Div - Tech"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank"]=="3rd Div - Estr"] = "3rd Div - Estr"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank"]=="Counsel Master"] = "Counsel Master"


filtro_cargo_alterar = np.logical_and(ANALYTICS["Status_presente_fc"]=="Presente", ANALYTICS["Rank"]!=ANALYTICS["Rank_recomendado"])
filtro_cargo_manter = np.logical_and(ANALYTICS["Status_presente_fc"]=="Presente", ANALYTICS["Rank"]==ANALYTICS["Rank_recomendado"])

ANALYTICS["Mudar_cargo"]= float("NaN")
ANALYTICS["Mudar_cargo"].loc[filtro_cargo_alterar == True] = "Alterar"
ANALYTICS["Mudar_cargo"].loc[filtro_cargo_manter == True] = "Manter"

ANALYTICS["Mudar_cargo"].loc[ANALYTICS["Mudar_cargo"].isnull()]= "N/A"
ANALYTICS["Rank_recomendado"].loc[ANALYTICS["Rank_recomendado"].isnull()]= "N/A"
ANALYTICS["Rank"].loc[ANALYTICS["Rank"].isnull()]= "N/A"

# %%
sup = list(ANALYTICS.columns)
sup[sup.index("Name_x")] = "Name"
ANALYTICS.columns = sup

# %%
print("Enriquencimento da Tabela Fato foi realizada.")
upload_s3("analytics.csv","client","aws-ffbucket",ANALYTICS)

# %%
PROD_Cargos = ANALYTICS.loc[np.logical_and(ANALYTICS["Status_presente_fc"]=="Presente",ANALYTICS["Mudar_cargo"]=="Alterar")]
PROD_Cargos.drop(["Data_entrada","Data_saida","Status_presente_fc","Mudar_cargo"],axis=1, inplace=True)
PROD_Cargos["Role_Jobs"] = PROD_Cargos["Role_Jobs"].astype(int)
PROD_Cargos["Qtd_Jobs"] = PROD_Cargos["Qtd_Jobs"].astype(int)
PROD_Cargos["Qtd_dias"] = PROD_Cargos["Qtd_dias"].astype(int)
PROD_Cargos["ID"] = PROD_Cargos["ID"].astype(int)
PROD_Cargos.set_index("ID", inplace=True)




# %%
print("Tabela tratada PROD_CARGOS gerada com sucesso.")
upload_s3("prod_cargos.csv","client","aws-ffbucket",PROD_Cargos)


