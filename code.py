import pyodbc
import pandas as pd
import pandas_dedupe
import warnings
import re

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=DESKTOP-RVC58B2;"
                      "Database=master;"
                      "Trusted_Connection=yes;")


cursor = cnxn.cursor()
requestString = 'Select * from outlets'
df = pd.read_sql_query(requestString, cnxn)
def clean(x):
    x = x.replace("\\", "")
    x = x.replace("/", "")
    x = x.replace("ООО", "")
    x = x.replace("ОАО", "")
    x = x.replace('ИП', "")
    x = x.replace('"', "")
    x = x.replace("'", "")
    #x = x.replace('.', "")
    #x = x.replace(',', "")
    x = x.strip()
    return x
df['Торг_точка_грязная'] = df['Торг_точка_грязная'].apply(clean)


def find_name(text):
    """Функция поиска ФИО"""
    text = str(text)  # Для уверенности переводим в строку
    text = text.replace('Ё', 'Е')
    text = text.replace('ё', 'е')
    text = re.sub(r'\s+', ' ', text)
    pattern = r'((\b[A-Я][^A-Я\s\.\,][a-я]*)(\s+)([A-Я][a-я]*)' + \
              '(\.+\s*|\s+)([A-Я][a-я]*))'
    name = re.findall(pattern, text)
    if name:
        return name[0][0]
    else:
        return text
#print(df['Торг_точка_грязная'][:105])
#requestString2 = "Select * from outlets WHERE Торг_точка_грязная_адрес NOT LIKE '%он же%'"
dict = {}
count = 1
for i, row in df.iterrows():
    address = str(row['Торг_точка_грязная_адрес'])
    name = str(row['Торг_точка_грязная'])
    name = find_name(name)
    if 'он же' not in address.lower() and '-' not in address and name not in dict.keys():
        dict[name] = count
        insertstatement = 'INSERT INTO outlets_clean VALUES (?, ?)'
        val = (count, address)
        cursor.execute(insertstatement, val)
        cnxn.commit()
        updatestatement = 'UPDATE outlets SET [outlet_clean_id] = ? WHERE [id] = ?'
        val = (count, int(row['id']))
        cursor.execute(updatestatement, val)
        cnxn.commit()
        count += 1
    elif name in dict.keys():
        updatestatement = '''UPDATE outlets SET [outlet_clean_id] = ? 
                              WHERE [id] = ?'''
        val = (dict[name], int(row['id']))
        cursor.execute(updatestatement, val)
        cnxn.commit()
print('Done')
for i, row in df.iterrows():
    address = str(row['Торг_точка_грязная_адрес'])
    name = str(row['Торг_точка_грязная'])
    if ('он же' in address.lower() or '-' in address) and name in dict.keys():
        updatestatement = '''UPDATE outlets SET [outlet_clean_id] = ? 
                                      WHERE [id] = ?'''
        val = (dict[name], int(row['id']))
        cursor.execute(updatestatement, val)
        cnxn.commit()
print('Done')
