#from src.utils.custom_func import my_inline_function
#my_inline_function('../data/', '../data/raw/S1_XX_F1_10000_DDMMYYYY.csv')
import json
config={
  "header": ["COLUMNA","COLUMNB","COLUMNC","COLUMND","COLUMNE","COLUMNF"],
  "input":"/content/drive/My Drive/data/",
  "outputsplit":True
}
with open('../data/raw/template.json', 'w') as outfile:
    json.dump(config, outfile)


#print(yy,mm,dd)