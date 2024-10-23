import re
from tqdm import tqdm
import pandas as pd
import json 
from DB_Operations import DB_Operations


class RuleBasedAddressParser:
    def AddressParser(line, key, values):

        MASK=[] #In String
     
        USAD_Conversion_Dict={"USAD_SNO":"","USAD_SPR":"","USAD_SPR":"","USAD_SNM":"","USAD_SFX":"","USAD_SPT":"","USAD_ANM":"","USAD_ANO":"","USAD_CTY":"","USAD_STA":"","USAD_ZIP":"","USAD_ZP4":"","USAD_BNM":"","USAD_BNO":"","USAD_RNM":"","USAD_RNO":"","USAD_HNM":"","USAD_HNO":"","USAD_MDG":"","USAD_MGN":"","USAD_NA":"", "USAD_ORG":""}
        List=USAD_Conversion_Dict.keys()
        FirstPhaseList=[]
        ck = key
        cv = values
        AddressList=line
        TrackKey=[]
        Mask=[]
        Combine=""
        Compare=False
        LoopCheck=1
        for A in AddressList:
            #print(A)
            FirstPhaseDict={}
            NResult=False
            try:
                Compare=A[0].isdigit()
            except:
                print()
            if A==",":
                O=0
                Mask.append(Combine)
                Combine=""
                #FirstPhaseList.append("Seperator")
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
        USAD_Mapping={"USAD_SNO":[],"USAD_SPR":[],"USAD_SNM":[],"USAD_SFX":[],"USAD_SPT":[],"USAD_ANM":[],"USAD_ANO":[],"USAD_CTY":[],"USAD_STA":[],"USAD_ZIP":[],"USAD_ZP4":[],"USAD_BNM":[],"USAD_BNO":[],"USAD_RNM":[],"USAD_RNO":[],"USAD_HNM":[],"USAD_HNO":[],"USAD_MDG":[],"USAD_MGN":[],"USAD_NA":[], "USAD_ORG":[]}
        Start = 0
        Counts = 0
        is_university = False
        org_name = []
        Final_Map=[]
        end=0
        for i in range(len(TrackKey)):
            if TrackKey[i] == 'Z' or TrackKey[i] not in ['N','S','T','D','X','R']:
                is_university = True
            if is_university:
                if TrackKey[i] in ['N','S','D','X','R']:
                    end=i
                    break
            if not is_university:
                break

        for i in range(end): 
            org_name.append(AddressList[i])       

        for i in range(0, len(org_name)):
            Final_Map.append([org_name[i],'USAD_ORG', TrackKey[i]])
        
      
        if org_name:
            USAD_Conversion_Dict["USAD_ORG"] = " ".join(org_name).strip()
            Counts = len(org_name)
            lst=[]
            for i in range(1,Counts):
                lst.append(i)
            USAD_Mapping["USAD_ORG"] = lst
            
        Dup_Address_List = AddressList
        Dup_FirstPhaseList = FirstPhaseList[:]
        
        
        # Remove elements from Dup_FirstPhaseList and Dup_Address_List
        for i in range(Counts):
            Dup_FirstPhaseList[i] = ""
            
        for i in range(len(TrackKey)-Counts):
            Final_Map.append("")
     
        
        # Counts+=1
        
        # print("******")
        if "X" not in TrackKey:
            
            for R in USAD_Conversion_Dict:
               
                if R=="USAD_SNO":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="N":
                            USAD_Mapping["USAD_SNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_SNO"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_SNO",Key]
                            Counts+=1
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_SNO"]=USAD_Mapping["USAD_SNO"]
                                USAD_Conversion_Dict["USAD_SNO"]=USAD_Conversion_Dict["USAD_SNO"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_SNO"]=USAD_Conversion_Dict["USAD_SNO"].strip()
                            break
                        
                elif R=="USAD_SPR":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="D":
                            USAD_Mapping["USAD_SPR"].append(j+1)
                            USAD_Conversion_Dict["USAD_SPR"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_SPR",Key]
                            Counts+=1
                           
                            Dup_Address_List.remove(Value.strip())
                           # Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                            
                        
                        
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_SPR"]=USAD_Mapping["USAD_SPR"]
                                USAD_Conversion_Dict["USAD_SPR"]=USAD_Conversion_Dict["USAD_SPR"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_SPR"]=USAD_Conversion_Dict["USAD_SPR"].strip()
                            break
    
                elif R=="USAD_SNM":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                       
                        
                        
                        if Key=="W" or Key=="N":
                            USAD_Mapping["USAD_SNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_SNM"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_SNM",Key]
                            Counts+=1
                            
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_SNM"]=USAD_Mapping["USAD_SNM"]
                                USAD_Conversion_Dict["USAD_SNM"]=USAD_Conversion_Dict["USAD_SNM"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_SNM"]=USAD_Conversion_Dict["USAD_SNM"].strip()
                            break
                      
                elif R=="USAD_SFX":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        
                        if Key=="F" or Key=="W" or Key=="S":
                            USAD_Mapping["USAD_SFX"].append(j+1)
                            USAD_Conversion_Dict["USAD_SFX"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_SFX",Key]
                            Counts+=1
                            
                            Dup_Address_List.remove(Value.strip())
                           # Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                            
                            
                        try:
                            if TrackKey[j+1]!="F":
                                USAD_Mapping["USAD_SFX"]=USAD_Mapping["USAD_SFX"]
    
                                USAD_Conversion_Dict["USAD_SFX"]=USAD_Conversion_Dict["USAD_SFX"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_SFX"]=USAD_Conversion_Dict["USAD_SFX"].strip()
                            break    
                        
                elif R=="USAD_SPT":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="D":
                            USAD_Mapping["USAD_SPT"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_SPT",Key]
                            Counts+=1
                            
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_SPT"]=USAD_Mapping["USAD_SPT"]
                                USAD_Conversion_Dict["USAD_SPT"]=USAD_Conversion_Dict["USAD_SPT"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_SPT"]=USAD_Conversion_Dict["USAD_SPT"].strip()
                            break
                elif R=="USAD_ANM":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="S":
                            USAD_Mapping["USAD_ANM"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_ANM",Key]
                            Counts+=1
                           # Final_Map.append([Value.strip(),"USAD_ANM",Key])
                            Dup_Address_List.remove(Value.strip())
                           # Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_ANM"]=USAD_Mapping["USAD_ANM"]
                                USAD_Conversion_Dict["USAD_ANM"]=USAD_Conversion_Dict["USAD_ANM"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_ANM"]=USAD_Conversion_Dict["USAD_ANM"].strip()
                            break
                elif R=="USAD_ANO":
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="N":
                            USAD_Mapping["USAD_ANO"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_ANO",Key]
                            Counts+=1
                            #Final_Map.append([Value.strip(),"USAD_ANO",Key])
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_ANO"]=USAD_Mapping["USAD_ANO"]
    
                                USAD_Conversion_Dict["USAD_ANO"]=USAD_Conversion_Dict["USAD_ANO"].strip()
                            break
                        except:
                            USAD_Conversion_Dict["USAD_ANO"]=USAD_Conversion_Dict["USAD_ANO"].strip()
                            break
                elif R=="USAD_CTY":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                            
                        if Key=="W":
                            USAD_Mapping["USAD_CTY"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_CTY",Key]
                            Counts+=1
                            #Final_Map.append([Value.strip(),"USAD_CTY",Key])
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                                
                            if TrackKey[j+1]!="W":
                                USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
        
                                USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
                            USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                        
                elif R=="USAD_STA":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        if Key=="T":
                            USAD_Mapping["USAD_STA"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_STA",Key]
                            Counts+=1
                           # Final_Map.append([Value.strip(),"USAD_STA",Key])
                            Dup_Address_List.remove(Value.strip())
                           # Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                            
                        try:
                                
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
        
                                USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
                            USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
    
                elif R=="USAD_ZIP":
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                        
                        if Key=="N":
                            USAD_Mapping["USAD_ZIP"].append(j+1)
                            Final_Map[j]=[Value.strip(),"USAD_ZIP",Key]
                            Counts+=1
                           # Final_Map.append([Value.strip(),"USAD_ZIP",Key])
                            Dup_Address_List.remove(Value.strip())
                            #Dup_FirstPhaseList.pop(j)
                            Dup_FirstPhaseList[j]=""
                        try:
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                                USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                                break
                        except:
                                USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                                USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                else:
                    for j in range(0,len(Dup_FirstPhaseList)):
                        if Dup_FirstPhaseList[j]!="":
                            

                            # print(Dup_FirstPhaseList[j])
                            Dictionary=Dup_FirstPhaseList[j]
                            Key=""
                            Value=""
                            for K,V in Dictionary.items():
                                Key=K
                                Value=V
                            USAD_Mapping["USAD_NA"].append(j+1)
                            USAD_Conversion_Dict["USAD_NA"]+=" "+Value.strip()
                         
                            Final_Map[j]=[Value.strip(),"USAD_NA",Key]
                            
                    break
                        
                    
        elif "X" in TrackKey:
            list_of_values=[]
            for d in FirstPhaseList:
                for v in d.values():
                    list_of_values.append(v)
            for R in USAD_Conversion_Dict:
                if  "HC" not in list_of_values and "RR" not in list_of_values:
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="X":
                            USAD_Mapping["USAD_BNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNM"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNM",Key]
                            Dup_FirstPhaseList[j]=""
                            
                        
            
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_BNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNO"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNO",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
        
                                USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
                            USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()

                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="W":
                            USAD_Mapping["USAD_CTY"].append(j+1)
                            USAD_Conversion_Dict["USAD_CTY"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_CTY",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="W":
                                USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
        
                                USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
                            USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()    
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="T":
                            USAD_Mapping["USAD_STA"].append(j+1)
                            USAD_Conversion_Dict["USAD_STA"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_STA",Key]
                            Dup_FirstPhaseList[j]=""
                        
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
        
                                USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
                            USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_ZIP"].append(j+1)
                            USAD_Conversion_Dict["USAD_ZIP"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_ZIP",Key]
                            Dup_FirstPhaseList[j]=""
                            
                        
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
        
                                USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                            USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                  
                            
                    for j in range(0,len(Dup_FirstPhaseList)):
                        if Dup_FirstPhaseList[j]!="":
                            # print(Dup_FirstPhaseList[j])
                            Dictionary=Dup_FirstPhaseList[j]
                            Key=""
                            Value=""
                            for K,V in Dictionary.items():
                                Key=K
                                Value=V
                    
                            USAD_Mapping["USAD_NA"].append(j+1)
                            USAD_Conversion_Dict["USAD_NA"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_NA",Key]
        
                elif "HC" in list_of_values:
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="F":
                            USAD_Mapping["USAD_HNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_HNM"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_HNM",Key]
                            Dup_FirstPhaseList[j]=""
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_HNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_HNO"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_HNO",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_HNO"]=USAD_Mapping["USAD_HNO"]
        
                                USAD_Conversion_Dict["USAD_HNO"]=USAD_Conversion_Dict["USAD_HNO"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_HNO"]=USAD_Mapping["USAD_HNO"]
                            USAD_Conversion_Dict["USAD_HNO"]=USAD_Conversion_Dict["USAD_HNO"].strip()

                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="X":
                            USAD_Mapping["USAD_BNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNM"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNM",Key]
                            Dup_FirstPhaseList[j]=""
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_BNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNO"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNO",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
        
                                USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
                            USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="W":
                            USAD_Mapping["USAD_CTY"].append(j+1)
                            USAD_Conversion_Dict["USAD_CTY"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_CTY",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="W":
                                USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
        
                                USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
                            USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()    
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="T":
                            USAD_Mapping["USAD_STA"].append(j+1)
                            USAD_Conversion_Dict["USAD_STA"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_STA",Key]
                            Dup_FirstPhaseList[j]=""
                        
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
        
                                USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
                            USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                
                
                    for j in range(Counts,len(TrackKey)):
                        # print(True)
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_ZIP"].append(j+1)
                            USAD_Conversion_Dict["USAD_ZIP"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_ZIP",Key]
                            Dup_FirstPhaseList[j]=""
                        
                        try:          
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                    
                                USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                            USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                    
                 
                            
                    for j in range(0,len(Dup_FirstPhaseList)):
                        if Dup_FirstPhaseList[j]!="":
                            Dictionary=Dup_FirstPhaseList[j]
                            Key=""
                            Value=""
                            for K,V in Dictionary.items():
                                Key=K
                                Value=V
                    
                            USAD_Mapping["USAD_NA"].append(j+1)
                            USAD_Conversion_Dict["USAD_NA"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_NA",Key]
                            
                elif "RR" in list_of_values:
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="R":
                            USAD_Mapping["USAD_RNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_RNM"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_RNM",Key]
                            Dup_FirstPhaseList[j]=""
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N" or Key=="D":
                            USAD_Mapping["USAD_RNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_RNO"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_RNO",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_RNO"]=USAD_Mapping["USAD_RNO"]
        
                                USAD_Conversion_Dict["USAD_RNO"]=USAD_Conversion_Dict["USAD_RNO"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_RNO"]=USAD_Mapping["USAD_RNO"]
                            USAD_Conversion_Dict["USAD_RNO"]=USAD_Conversion_Dict["USAD_RNO"].strip()

                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="X":
                            USAD_Mapping["USAD_BNM"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNM"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNM",Key]
                            Dup_FirstPhaseList[j]=""
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_BNO"].append(j+1)
                            USAD_Conversion_Dict["USAD_BNO"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_BNO",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
        
                                USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_BNO"]=USAD_Mapping["USAD_BNO"]
                            USAD_Conversion_Dict["USAD_BNO"]=USAD_Conversion_Dict["USAD_BNO"].strip()
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="W" or Key=="D":
                            USAD_Mapping["USAD_CTY"].append(j+1)
                            currentValue = USAD_Conversion_Dict["USAD_CTY"]
                            StrippedValue = Value.strip()+" "
                            if currentValue:
                                appendedValue = currentValue + " " +StrippedValue
                            else:
                                appendedValue = StrippedValue
                            USAD_Conversion_Dict["USAD_CTY"] = appendedValue
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_CTY",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="W":
                                USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
        
                                USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_CTY"]=USAD_Mapping["USAD_CTY"]
                            USAD_Conversion_Dict["USAD_CTY"]=USAD_Conversion_Dict["USAD_CTY"].strip()
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="T":
                            USAD_Mapping["USAD_STA"].append(j+1)
                            USAD_Conversion_Dict["USAD_STA"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_STA",Key]
                            Dup_FirstPhaseList[j]=""
                        
                        try:          
                            if TrackKey[j+1]!="T":
                                USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
        
                                USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_STA"]=USAD_Mapping["USAD_STA"]
                            USAD_Conversion_Dict["USAD_STA"]=USAD_Conversion_Dict["USAD_STA"].strip()
                
                
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_ZIP"].append(j+1)
                            USAD_Conversion_Dict["USAD_ZIP"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_ZIP",Key]
                            Dup_FirstPhaseList[j]=""
                        
                        try:          
                            if TrackKey[j+1]!="N":
                                USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                   
                                USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_ZIP"]=USAD_Mapping["USAD_ZIP"]
                            USAD_Conversion_Dict["USAD_ZIP"]=USAD_Conversion_Dict["USAD_ZIP"].strip()
                   
             
                            
                    for j in range(0,len(Dup_FirstPhaseList)):
                        if Dup_FirstPhaseList[j]!="":
                            Dictionary=Dup_FirstPhaseList[j]
                            Key=""
                            Value=""
                            for K,V in Dictionary.items():
                                Key=K
                                Value=V
                    
                            USAD_Mapping["USAD_NA"].append(j+1)
                            USAD_Conversion_Dict["USAD_NA"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_NA",Key]
                            
                elif "FPO" and "M" in list_of_values:
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="M":
                            USAD_Mapping["USAD_MDG"].append(j+1)
                            USAD_Conversion_Dict["USAD_MDG"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_MDG",Key]
                            Dup_FirstPhaseList[j]=""
                
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="W":
                            USAD_Mapping["USAD_MGN"].append(j+1)
                            currentValue = USAD_Conversion_Dict["USAD_MGN"]
                            StrippedValue = Value.strip()+" "
                            if currentValue:
                                appendedValue = currentValue + " " +StrippedValue
                            else:
                                appendedValue = StrippedValue
                            USAD_Conversion_Dict["USAD_MGN"] = appendedValue
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_MGN",Key]
                            Dup_FirstPhaseList[j]=""
                        try:          
                            if TrackKey[j+1]!="W":
                                USAD_Mapping["USAD_MGN"]=USAD_Mapping["USAD_MGN"]
        
                                USAD_Conversion_Dict["USAD_MGN"]=USAD_Conversion_Dict["USAD_MGN"].strip()
                                break
                        except:
                            USAD_Mapping["USAD_MGN"]=USAD_Mapping["USAD_MGN"]
                            USAD_Conversion_Dict["USAD_MGN"]=USAD_Conversion_Dict["USAD_MGN"].strip()
                
                    
                    for j in range(Counts,len(TrackKey)):
                        Dictionary=FirstPhaseList[j]
                        Key=""
                        Value=""
                        for K,V in Dictionary.items():
                            Key=K
                            Value=V
                            
                        if Key=="N":
                            USAD_Mapping["USAD_ZIP"].append(j+1)
                            USAD_Conversion_Dict["USAD_ZIP"]+=Value.strip()+" "
                            Counts+=1
                            Final_Map[j]=[Value.strip(),"USAD_ZIP",Key]
                            Dup_FirstPhaseList[j]=""
                    
                            
                    for j in range(0,len(Dup_FirstPhaseList)):
                        if Dup_FirstPhaseList[j]!="":
                            # print(Dup_FirstPhaseList[j])
                            Dictionary=Dup_FirstPhaseList[j]
                            Key=""
                            Value=""
                            for K,V in Dictionary.items():
                                Key=K
                                Value=V
                    
                            USAD_Mapping["USAD_NA"].append(j+1)
                            USAD_Conversion_Dict["USAD_NA"]+=" "+Value.strip()
                            Final_Map[j]=[Value.strip(),"USAD_NA",Key]
                     
                     
        dic = {key:value for key,value in USAD_Conversion_Dict.items() if value != ''}
        return Final_Map

