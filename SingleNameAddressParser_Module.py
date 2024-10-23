# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 00:42:19 2022

@author: onais
"""


import re
from tqdm import tqdm
import pandas as pd
import json 
#Parsing 1st program

def NameandAddressParser(line): 
    Percentage_of_parsed=0

    Final_Output={}
    fileHandle = open('USAddressWordTable.txt', 'r',encoding="utf8")
    NamefileHandle = open('NamesWordTableOpt.txt', 'r',encoding="utf8")
    SplitWordTable = open('SplitWordTable.txt', 'r',encoding="utf8")
    AllAddress_Key_Value_As_MASK_Comp={}
    Observation=0
    Total=0
    dataFinal={}
    USAD_Conversion_Dict={"1":"USAD_SNO","2":"USAD_SPR","3":"USAD_SNM","4":"USAD_SFX","5":"USAD_SPT","6":"USAD_ANM","7":"USAD_ANO","8":"USAD_CTY","9":"USAD_STA","10":"USAD_ZIP","11":"USAD_ZP4","12":"USAD_BNM","13":"USAD_BNO","14":"USAD_RNM"}
    FirstPhaseList=[]
    Address=re.sub(',',' , ',line)
    Address=re.sub(' +', ' ',Address)
    Address=re.sub('[.]','',Address)
    #Address=re.sub('#','',Address)    
    Address=Address.upper()
    AddressList = re.split("\s|\s,\s ", Address)
    tmp1=0
    NameList=[]
    RevisedAddressList=[]
    SplitMask=""
    
    for A in AddressList:
        FirstPhaseDict={}
        NResult=False
        try:
            Compare=A[0].isdigit()
        except:
            a=0
        if A==",":
            SplitMask+=","
        elif Compare:
            SplitMask+="A"
        else:
            NR=True
            for line in SplitWordTable:
            
                fields=line.split('|')
                if A==(fields[0]):
                    SplitMask+=fields[1].strip()
                    NR=False
                    break
            if NR:
                SplitMask+="W"
        SplitWordTable.seek(0)
    indexSplit=0
    for m in range(len(SplitMask)):
        if SplitMask[m] in ("W","P",",") :
            continue
        else:
            indexSplit=m
            break
    
    RevisedAddressList = AddressList[indexSplit:len(AddressList)]
    NameList = AddressList[0:indexSplit]
    
    if NameList[len(NameList)-1]==",":
        NameList.pop(len(NameList)-1)
    for i in AddressList:
        if i=="PO" or i=="POBOX":
            RevisedAddressList = AddressList[tmp1:len(AddressList)]
            NameList = AddressList[0:tmp1]
            break
        try:
            Compare=i.isdigit()
        except:    
            a=0
        if Compare==True:
            RevisedAddressList = AddressList[tmp1:len(AddressList)]
            NameList = AddressList[0:tmp1]
            break
        tmp1+=1
    if NameList[len(NameList)-1]==",":
        NameList.pop(len(NameList)-1)
    print(NameList)
    print(RevisedAddressList)
    #del(AddressList[len(AddressList)-1])
    TrackKey=[]
    Mask=[]
    Combine=""
    LoopCheck=1
    for A in RevisedAddressList:
        FirstPhaseDict={}
        NResult=False
        try:
            Compare=A[0].isdigit()
        except:
            a=0
        if A==",":
            O=0
            Mask.append(Combine)
            Combine=""
            FirstPhaseList.append(",")
            #FirstPhaseList.append("Seperator")
        elif Compare:
            Combine+="N"
            TrackKey.append("N")
            FirstPhaseDict["N"]=A
            FirstPhaseList.append(FirstPhaseDict)
        else:
            for line in fileHandle:
                fields=line.split('|')
                if A==(fields[0]):
                    NResult=True
                    temp=fields[1]
                    Combine+=temp[0]
                    FirstPhaseDict[temp[0]] = A
                    FirstPhaseList.append(FirstPhaseDict)
                    TrackKey.append(temp[0])
            if NResult==False:
                Combine+="W"
                TrackKey.append("W")
                FirstPhaseDict["W"] = A
                FirstPhaseList.append(FirstPhaseDict)
        if LoopCheck==len(RevisedAddressList):
            Mask.append(Combine)
        fileHandle.seek(0)
        LoopCheck+=1    
    FirstPhaseListAddress=FirstPhaseList
    Mask_1=",".join(Mask)
    USAD_Mapping={"USAD_SNO":[],"USAD_SPR":[],"USAD_SPR":[],"USAD_SNM":[],"USAD_SFX":[],"USAD_SPT":[],"USAD_ANM":[],"USAD_ANO":[],"USAD_CTY":[],"USAD_STA":[],"USAD_ZIP":[],"USAD_ZP4":[],"USAD_BNM":[],"USAD_BNO":[],"USAD_RNM":[]}
    Start=0
    Counts=0
    FirstPhase_WithComma=FirstPhaseList
    FirstPhaseList = [FirstPhaseList[b] for b in range(len(FirstPhaseList)) if FirstPhaseList[b] != ","]
    data={}
    with open('JSONMappingDefault.json', 'r+', encoding='utf-8') as f:
        data = json.load(f)
    Found=False
    FoundDict={}
    for tk,tv in data.items():
        if(tk==Mask_1):
            FoundDict[tk]=tv
            Found=True
            break
    FoundExcept=False
    with open('ExceptionFile.json', 'r+', encoding='utf-8') as g:
        Stat = json.load(g)
        if Mask_1 in Stat.keys():
            FoundExcept=True
    FirstPhaseList=[]
    NameTrackKey=[]
    Mask=[]
    Combine=""
    LoopCheck=1
    for A in NameList:
        FirstPhaseDict={}
        NResult=False
        if A==",":
            O=0
            Mask.append(Combine)
            Combine=""
            FirstPhaseList.append(",")
            #FirstPhaseList.append("Seperator")
        elif A==" ":
            continue
        elif A!="," and len(A)==1:
            Combine+="I"
            TrackKey.append("I")
            FirstPhaseDict["I"] = A
            FirstPhaseList.append(FirstPhaseDict)
        else:
            for line in NamefileHandle:
                fields=line.split('|')
                if A==(fields[0]):
                    NResult=True
                    temp=fields[1]
                    Combine+=temp[0]
                    FirstPhaseDict[temp[0]] = A
                    FirstPhaseList.append(FirstPhaseDict)
                    TrackKey.append(temp[0])
                    break
            if NResult==False:
                Combine+="W"
                TrackKey.append("W")
                FirstPhaseDict["W"] = A
                FirstPhaseList.append(FirstPhaseDict)
        if LoopCheck==len(NameList):
            Mask.append(Combine)
        NamefileHandle.seek(0)
        LoopCheck+=1
    
    FirstPhaseListName=FirstPhaseList
    NameMask_1=",".join(Mask)
    FirstPhaseListName = [FirstPhaseListName[b] for b in range(len(FirstPhaseListName)) if FirstPhaseListName[b] != ","]
    data={}
    with open('JSONMappingNameDefault.json', 'r+', encoding='utf-8') as f:
        data = json.load(f)
    FoundName=False
    NameFoundDict={}
    for tk,tv in data.items():
        if(tk==NameMask_1):
            NameFoundDict[tk]=tv
            FoundName=True
            break
    FoundExceptname=False
    with open('NameExceptionFile.json', 'r+', encoding='utf-8') as g:
        Stat = json.load(g)
        if NameMask_1 in Stat.keys():
            FoundExceptname=True
    FirstPhaseListAddress = [FirstPhaseListAddress[b] for b in range(len(FirstPhaseListAddress)) if FirstPhaseListAddress[b] != ","]
    print(FoundName)
    if Found and FoundName:
        Observation+=1
        AllAddress_Key_Value_As_MASK_Comp={}
        Mappings={"Name":{},"Address":{}}
        for K2,V2 in FoundDict[Mask_1].items():
            Temp=""
            for p in V2:
                for K3,V3 in FirstPhaseListAddress[p-1].items():
                   Temp+=" "+V3
                   Temp=Temp.strip()
                   Mappings["Address"][K2]=Temp
        for K2,V2 in NameFoundDict[NameMask_1].items():
            Temp=""
            for p in V2:
                for K3,V3 in FirstPhaseListName[p-1].items():
                   Temp+=" "+V3
                   Temp=Temp.strip()
                   Mappings["Name"][K2]=Temp
        CombinedMask=Mask_1+","+NameMask_1
        try:
            Final_Output["Output"]=Mappings
        except:
            Final_Output["Output"]=Mappings
            dataFinal[CombinedMask]={}
    elif not FoundExcept:  
        with open('ExceptionFile.json', 'r+', encoding='utf-8') as g:
            Stat = json.load(g)
            Stat[Mask_1]=FirstPhaseListAddress
            g.seek(0)
            json.dump(Stat,g,indent=4)
            g.truncate
    elif not FoundExceptname:  
        with open('NameExceptionFile.json', 'r+', encoding='utf-8') as g:
            Stat = json.load(g)
            Stat[NameMask_1]=FirstPhaseListName
            g.seek(0)
            json.dump(Stat,g,indent=4)
            g.truncate
        
        Total+=1
    return Final_Output
# print("Address Matching Report")
# print("Total=",Count)
# print("Matched Addresses=",Observation)
# print("Percentage of Matched",(Observation/Count)*100)
        # print("Mask Generated is ",Mask_1)
    # print("Index\tMaskToken\t\tAddress Token")
    # i=1
    # for k in FirstPhaseList:
    #     for key,value in k.items():
    #         print(i,"\t\t",key,"\t\t\t\t",value) 
    #     i+=1