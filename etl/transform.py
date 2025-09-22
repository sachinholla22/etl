import pandas as pd
import logging 

def transform_csv(csv_path):
    try:
        df=pd.read_csv(csv_path)
        df.columns.str.strip()
        df['date_of_birth']=pd.to_datetime(df['date_of_birth'],format="mixed")
        df['registration_date']=pd.to_datetime(df['registration_date'],format="mixed")

        def formatGender(gender):
            gender=gender.lower()
            if gender in ['female','f','fem@le']:
                return "F"
            elif gender in ['male','m']:
                return "M"
            else:
                return "O"    
        df['gender']=df['gender'].astype(str)        
        df['gender']=df['gender'].apply(formatGender) 

        def capitalizeName(name):
            name=name.lower()
            name=name.capitalize()
            return name
        df['first_name']=df['first_name'].astype(str)    
        df['last_name']=df['last_name'].astype(str)  
        df['first_name']=df['first_name'].apply(capitalizeName)
        df['last_name'] =df['last_name'].apply(capitalizeName)
        df=df.dropna()
        cleaned_file=csv_path.replace(".csv","_cleaned.csv")
        df.to_csv(cleaned_file,index=False)
        return cleaned_file
    except:
        logging.info("Error while transforming ")
        return None