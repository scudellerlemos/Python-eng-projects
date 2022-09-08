# %%
import pandas as pd
import requests as req
import boto3
import io

# %%
def dados_FC():
    response = req.get("https://xivapi.com/freecompany/9234349560946614930?data=FCM")
    return response.json()

# %%
MEMBROS_FC = pd.DataFrame(dados_FC()["FreeCompanyMembers"])
MEMBROS_FC.drop(["Lang","RankIcon","FeastMatches","Server"],axis = 1, inplace = True)

MEMBROS_FC["ID"] = MEMBROS_FC["ID"].astype(str) 
MEMBROS_FC["Lodestone"] = "https://na.finalfantasyxiv.com/lodestone/character/" + MEMBROS_FC["ID"] 
MEMBROS_FC["ID"] = MEMBROS_FC["ID"].astype(float) 


# %%

def upload_s3(file,paste,df):
    s3_file_key = str(paste)+"/"+str(file)
    s3 = boto3.client("s3",aws_access_key_id="AKIA3KLMGFFOZQKIY4VF", aws_secret_access_key="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")
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, header=True, index = False)
    csv_buf.seek(0)
    s3.put_object(Bucket="aws-ffbucket",Body=csv_buf.getvalue(),Key=s3_file_key)

# %%
upload_s3("membros_fc.csv","client",MEMBROS_FC)
print("projeto terminado com sucesso!")


