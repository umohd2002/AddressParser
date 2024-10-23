# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 12:50:21 2023

 

@author: onais
"""

import re

 

 

class PreProcessingNameAddress:
    def __init__(self):
        self
    def AddresssCleaning(self,line):
        line=line.lstrip(',')
        line=re.sub(r'[^a-zñáéíóúüÑÁÉÍÓÚÜA-Z0-9\s#,-]+', '',line)

        Address=re.sub(' +', ' ',line)
        Address=re.sub(',',' , ',Address)
        # Address=re.sub('#',' # ',Address)
        
        Address=Address.upper()

        AddressList = re.split("\s|\s,\s", Address)
        try:
            AddressList.remove("")
        except:
            True


        return (AddressList,Address)
    
# cleaning = PreProcessingNameAddress.AddresssCleaning(0,"1701 westpark drive, apt #&%105, Little Rock, AR 72204")
# print(cleaning)