# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 20:27:31 2023

@author: Salman Khan
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import json
from ORM import MaskTable, ComponentTable, MappingJSON, User, UserRole, ExceptionTable, MapCreationTable
# from LoginORM import UserRole, User
import re
import io
from tqdm import tqdm
import Rulebased as RuleBased
import pandas as pd
import json 
import collections 
import PreprocessingNameAddress as PreProc
import sklearn
from sklearn.metrics import multilabel_confusion_matrix,confusion_matrix,classification_report
from flask import session

#Parsing 1st program
from DB_Operations import DB_Operations
import zipfile
import os
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
today=datetime.today()
current_time = datetime.now().time()

# Format the time as HH:MM:SS
time_string = current_time.strftime("%H:%M:%S")

def Address_Parser(Address_4CAF50,Progress,TruthSet=""):
    Result={}
    RuleBasedOutput={}
    Detailed_Report=""
    Mask_log={}
    Unique_Mask={}
    db_operations = DB_Operations(database_url='sqlite:///KnowledgeBase.db')
    Address_4CAF50=open(Address_4CAF50,"r", encoding="utf-8")
    file_name = os.path.splitext(os.path.basename(Address_4CAF50.name))[0]
    Lines = Address_4CAF50.readlines()
    
    clue = db_operations.get_clue_data_as_dict()
    ck = list(clue.keys())
    cv = list(clue.values())
    Observation=0
    Total=0
    Truth_Result={}
    dataFinal={} 
    data={}
    data = db_operations.get_data_for_all()
    component_dict = {}
    component_dict = db_operations.get_components()
    Detailed_Report+="Exception and Mask Report\n"
    ExceptionList = []
    CNT=100/len(Lines)
    CN=0
    for line in tqdm(Lines, desc="Processing"):
        CN=CN+CNT
        # Progress["value"]=CN
        line=line.strip("\n").split("|")
        ID=line[0].strip()
        try:
            
            line=line[1].strip()
        except:
            continue
        Address=line
        FirstPhaseList=[]
        PackAddress=PreProc.PreProcessingNameAddress().AddresssCleaning(line)
        AddressList=PackAddress[0]
        AddressList = [i for i in AddressList if i]
      
        TrackKey=[]
        Mask=[]
        Combine=""
        LoopCheck=1
        for A in AddressList:
            FirstPhaseDict={}
            NResult=False
            try:
                Compare=A[0].isdigit()
            except:
                print()
            if A==",":
                Mask.append(Combine)
                Combine=""
                FirstPhaseList.append(",")
            elif Compare:
                Combine+="N"
                TrackKey.append("N")
                FirstPhaseDict["N"]=A
                FirstPhaseList.append(FirstPhaseDict)

            else:
                  
                  for k in range(len(ck)):
                      if A ==ck[k]:
                          temp=cv[k]
                          NResult=True
                          Combine+=temp
                          FirstPhaseDict[temp] = A 
                          FirstPhaseList.append(FirstPhaseDict)
                          TrackKey.append(temp)
                  if NResult==False:
                      Combine+="W"
                      TrackKey.append("W")
                      FirstPhaseDict["W"] = A
                      FirstPhaseList.append(FirstPhaseDict)
            if LoopCheck==len(AddressList):
                Mask.append(Combine)
           
            LoopCheck+=1
            
        Mask_1=",".join(Mask)
        Mask_log[ID]=Mask_1
        Unique_Mask[Mask_1]=ID
        FirstPhaseList = [FirstPhaseList[b] for b in range(len(FirstPhaseList)) if FirstPhaseList[b] != ","]
        Found=False
        FoundDict={}

        if Mask_1 in data.keys():
            FoundDict[Mask_1]=data[Mask_1]
            Found=True
        
        FoundExcept=False
        sorted_Found = {}
        if Found:
            Observation+=1
            Mappings=[]
            for K2,V2 in FoundDict[Mask_1].items():
                FoundDict_KB=FoundDict[Mask_1]
                sorted_Found={k: v for k ,v in sorted(FoundDict_KB.items(), key=lambda item:item[1])}
            for K2,V2 in sorted_Found.items():
                Temp=""
                Merge_token=""
                for p in V2:
                    for K3,V3 in FirstPhaseList[p-1].items():
                       Temp+=" "+V3
                       Temp=Temp.strip()
                       Merge_token+=K3
                       found = False
                       for entry in Mappings:
                           if entry[0] == K2:
                               # Append V3 to existing entry
                               entry[1] += K3
                               entry[2] = ""
                               entry[2] += Temp
                               found = True
                               break
                       if not found:
                         # Add a new entry to Mappings
                           Mappings.append([K2, K3, V3])

            FoundDict_KB=FoundDict[Mask_1]
            sorted_Found={k: v for k ,v in sorted(FoundDict_KB.items(), key=lambda item:item[1])}
            
            
            OutputEntry = {
                "Record ID": ID,
                "INPUT": Address,
                str(Mask_1): Mappings
            }
            OutputList = []
            OutputList.append(OutputEntry)
            
            try:
                Truth_Result[ID]=Mappings
                Result[ID]=OutputEntry
                dataFinal[Mask_1][ID] =Mappings # <--- add `id` value.
                
            except: 
                Result[ID]=OutputEntry
                Truth_Result[ID]=Mappings
                dataFinal[Mask_1]={}
                dataFinal[Mask_1][ID]=Mappings
            # OutputEntry = {}        
            
        elif not FoundExcept :
            rules=RuleBased.RuleBasedAddressParser.AddressParser(AddressList, ck, cv)
            for m in rules:
                component = m[1]
                if component not in component_dict.keys():
                    component_description = "Not Selected"
                    m[1] = "USNM_NA"
                    m.append(component_description)
                else:
                    component_description = component_dict[component]
                    m.append(component_description)

            ExceptionEntry = {
                "Record ID": ID,
                "INPUT": Address,
                str(Mask_1): rules
            }
            ExceptionList.append(ExceptionEntry)
            RuleBasedOutput[ID]=rules
        else:
            try:
                RuleBasedOutput[ID]=RuleBased.RuleBasedAddressParser.AddressParser(AddressList)
                Exception_Mask+=Mask_1+"\n"
            except:
                continue
        Total+=1

    percentage = (Observation/Total)*100
    percentage = "%.2f"% percentage
    Detailed_Report="\nTotal Number of Addresses: -\t"+"{:,}".format(Total)+""
    Detailed_Report+="\nUnique Pattern Count: -\t"+"{:,}".format(len(Unique_Mask))+"\n\n"
    Detailed_Report+="\nNumber of Pattern Parsed Addresses: -\t"+"{:,}".format(Observation)+"\n"
    Detailed_Report+="Percentage of Patterns Parsed Result:  -\t"+"{:.2f}%".format(float(percentage))+"\n"
    Detailed_Report+="\nNumber of Exceptions Thrown: -\t\t"+"{:,}".format(Total-Observation)+"\n"
    Detailed_Report+="Percentage of RuleBased Parsed Result: -\t"+"{:.2f}%".format(100-float(percentage))+"\n"


    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    detailed_report_stream = io.BytesIO()
    active_learning_stream = io.BytesIO()
    rule_based_output_stream = io.BytesIO()

    # Write the contents to the byte streams
    detailed_report_stream.write(Detailed_Report.encode('utf-8'))
    active_learning_stream.write(json.dumps(Result, ensure_ascii=False).encode('utf-8'))
    rule_based_output_stream.write(json.dumps(RuleBasedOutput, ensure_ascii=False).encode('utf-8'))
    detailed_report_stream.seek(0)
    active_learning_stream.seek(0)
    rule_based_output_stream.seek(0)

    # File names
    detailed_report_file_name = f"Detailed Report_{file_name}.txt"
    active_learning_file_name = f"Active Learning Output.json"
    rule_based_output_file_name = f"Rule Based Output.json"
    zip_file_name = f"Output/Batch File Output/{file_name}_output.zip"
    try:
    # Create a zip file and write the byte streams to it
        with zipfile.ZipFile(zip_file_name, 'w') as zipf:
            zipf.writestr(detailed_report_file_name, detailed_report_stream.getvalue())
            zipf.writestr(active_learning_file_name, active_learning_stream.getvalue())
            zipf.writestr(rule_based_output_file_name, rule_based_output_stream.getvalue())
    except Exception as e:
        print(f"Error creating zip file: {e}")
    
    Session = sessionmaker(create_engine('sqlite:///KnowledgeBase.db'))
    sessions = Session()
    mapdata_list = []
    exc_data_list = []
    try:

        for i in ExceptionList:
            rules = i
            excdata = {
                "Timestamp": timestamp,
                # "Username": session["user_id"],
                "Username": "admin",
                "Run": "Multiple",
                "Record ID": rules["Record ID"],
                "data": rules[next((key for key, value in rules.items() if isinstance(value, list)), None)]
            }
            mapdata = MapCreationTable(Address_Input=rules["INPUT"], Mask=next((key for key, value in rules.items() if isinstance(value, list)), None))
            mapdata_list.append(mapdata)
            # Wait until the mapdata object is added to get the ID
            sessions.add(mapdata)
            sessions.flush()  # Required to generate the ID for mapdata before using it
            j = 1
            for data in excdata["data"]:
                exc_data = ExceptionTable(
                    UserName=excdata["Username"], 
                    Timestamp=excdata["Timestamp"], 
                    Run=excdata["Run"], 
                    Address_ID=excdata["Record ID"], 
                    Component=data[1], 
                    Token=data[0], 
                    Mask_Token=data[2], 
                    Component_index=j, 
                    MapCreation_Index=mapdata.ID
                )
                exc_data_list.append(exc_data)
                j += 1
        # Add all the records in a batch
        sessions.add_all(mapdata_list + exc_data_list)
        # Commit the transaction
        sessions.commit()

    except IntegrityError as e:
        sessions.rollback()
        print(f"Error while committing: {e}")
    finally:
        sessions.close()
    return (True,f"Detailed_Report of {file_name}.txt is Generated! \n\nThe {file_name}_Output.zip is downloaded, please check your download's directory. \n\n{Detailed_Report}", zip_file_name)