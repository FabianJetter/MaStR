# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 11:50:11 2019

@author: FJetter
"""

import pandas as pd
import numpy as np
import zeep
from zeep import Client, Settings
from zeep.helpers import serialize_object
import time
import os

import loggin as log




apiKey = log.apiKey
myMastrNr = log.myMastrNr
myMarktrolleNr = log.myMarktrolleNr


wsdl = log.wsdl 
client = Client(wsdl=wsdl)
client_bind = client.bind('Marktstammdatenregister','Anlage')


path_Einheiten_folder = 'Q:/10_Regionenmodell/eegdb/MaStR/Daten/test/'


Einheittyp = [
        'Windeinheit',
        'Solareinheit',
        'Biomasse',
        'Wasser',
        'Geothermie',
        'Gaserzeugungseinheit',
        'Gasverbrauchseinheit',
        'Gasspeichereinheit',
        'Kernenergie',
        'Stromspeichereinheit',
        'Stromverbrauchseinheit',
        'Verbrennung'
        ]

Einheittyp_name = {
       'Windeinheit'            : 'Windeinheit',
       'Solareinheit'           : 'Solareinheit',
       'Biomasse'               : 'Biomasse',
       'Wasser'                 : 'Wasser',
       'Geothermie'             : 'Geothermie',
       'Gaserzeugungseinheit'   : 'Gaserzeugungseinheit',
       'Gasverbrauchseinheit'   : 'Gasverbrauchseinheit',
       'Gasspeichereinheit'     : 'Gasspeichereinheit',  
       'Kernenergie'            : 'Kernenergie',
       'Stromspeichereinheit'   : 'Stromspeichereinheit',
       'Stromverbrauchseinheit' : 'Stromverbrauchseinheit',
       'Verbrennung'            : 'Verbrennung'
       }

Einheittyp_operation = {
       'Windeinheit'            : client_bind.GetEinheitWind,
       'Solareinheit'           : client_bind.GetEinheitSolar,
       'Biomasse'               : client_bind.GetEinheitBiomasse,
       'Wasser'                 : client_bind.GetEinheitWasser,
       'Geothermie'             : client_bind.GetEinheitGeoSolarthermieGrubenKlaerschlamm,
       'Gaserzeugungseinheit'   : client_bind.GetEinheitGasErzeuger,
       'Gasverbrauchseinheit'   : client_bind.GetEinheitGasVerbraucher,
       'Gasspeichereinheit'     : client_bind.GetEinheitGasSpeicher,  
       'Kernenergie'            : client_bind.GetEinheitKernkraft,
       'Stromspeichereinheit'   : client_bind.GetEinheitStromSpeicher,
       'Stromverbrauchseinheit' : client_bind.GetEinheitStromVerbraucher,
       'Verbrennung'            : client_bind.GetEinheitVerbrennung
       }

Einheittyp_EEGoperation = {
        'Windeinheit'     : client_bind.GetAnlageEegWind,
        'Solareinheit'    : client_bind.GetAnlageEegSolar,
        'Biomasse'        : client_bind.GetAnlageEegBiomasse,
        'Wasser'          : client_bind.GetAnlageEegWasser,
        'Geothermie'      : client_bind.GetAnlageEegGeoSolarthermieGrubenKlaerschlamm
        }

Einheittyp_LOKATIONoperation = {
        'Windeinheit'            : client_bind.GetLokationStromErzeuger,
        'Solareinheit'           : client_bind.GetLokationStromErzeuger,
        'Biomasse'               : client_bind.GetLokationStromErzeuger,
        'Wasser'                 : client_bind.GetLokationStromErzeuger,
        'Geothermie'             : client_bind.GetLokationStromErzeuger,
        'Gaserzeugungseinheit'   : client_bind.GetLokationGasErzeuger,
        'Gasverbrauchseinheit'   : client_bind.GetLokationGasVerbraucher,
        'Stromspeichereinheit'   : client_bind.GetLokationStromErzeuger,
        'Stromverbrauchseinheit' : client_bind.GetLokationStromVerbraucher,
        'Verbrennung'            : client_bind.GetLokationStromErzeuger
        }

Einheittyp_KWKoperation = {
        'Biomasse'               : client_bind.GetAnlageKwk
        }

list_eeg = list()
for i in Einheittyp_EEGoperation.keys():
    list_eeg.append(i)

list_lokation = list()
for i in Einheittyp_LOKATIONoperation.keys():
    list_lokation.append(i)
          
list_kwk = list()
for i in Einheittyp_KWKoperation.keys():
    list_kwk.append(i)
    
GetOperator = ['Einheit','AnlageEEG','Lokation','AnlageKwk']


def Get(Einheittyp, Stand, EinheitMastrNummerList= None, GetOperatorList=None, nrows=None):
    path_alleEinheiten_folder  = 'Q:/10_Regionenmodell/eegdb/MaStR/Daten/alleEinheiten/' + Stand + '_alleEinheiten.csv'
    path = path_Einheiten_folder + Stand + '_' + Einheittyp_name[Einheittyp]
    
    if EinheitMastrNummerList == None: 
        df_alleEinheiten = pd.read_csv(path_alleEinheiten_folder, sep=';')
        MastrNrList  = df_alleEinheiten.loc[df_alleEinheiten['Einheittyp'] == Einheittyp_name[Einheittyp]].EinheitMastrNummer.values.tolist()
    else:
        MastrNrList = EinheitMastrNummerList
    with open(path + 'Einheiten.csv', 'a') as f, open(path + 'ErrorEinheiten.csv', 'a') as ef, open(path + 'EegAnlagen.csv', 'a') as g, open(path + 'ErrorEegAnlagen.csv', 'a') as eg, open(path + 'Lokationen.csv', 'a') as l, open(path + 'ErrorLokationen.csv', 'a') as el, open(path + 'KwkAnlagen.csv', 'a') as k, open(path + 'ErrorKwkAnlagen.csv', 'a') as ek:
        for i in range(0, nrows or len(MastrNrList), 1):  
             
            try:
                c = Einheittyp_operation[Einheittyp](apiKey=apiKey, marktakteurMastrNummer=myMastrNr, einheitMastrNummer=MastrNrList[i])
                s = serialize_object(c)
                df = pd.DataFrame(list(s.items()), columns= list('ab'))
                Einheit = df.set_index('a').transpose()
                if 'Einheit' in GetOperatorList or GetOperatorList == None:
                    Einheit.to_csv(f,  header=False, sep=';', index=False, encoding='utf-8')
                print (Einheittyp_name[Einheittyp] + ' ' + MastrNrList[i]  + ': ' + str(round(i/len(MastrNrList)*100,2)) + ' %; Anzahl: ' + str(i) + " von " + str(len(MastrNrList)))
            except:
                pd.DataFrame({'EinheitMastrNummer': [MastrNrList[i]]}).to_csv(ef,  header=False, sep=';', index=False, encoding='utf-8')
                pass
                
            if  ('AnlageEEG' in GetOperator or GetOperatorList == None) and Einheittyp_name[Einheittyp] in list_eeg:
                try:
                    EegMastrNummer = Einheit.EegMastrNummer.values[0]
                    c_eeg = Einheittyp_EEGoperation[Einheittyp](apiKey=apiKey, marktakteurMastrNummer=myMastrNr, eegMastrNummer=EegMastrNummer)
                    s_eeg = serialize_object(c_eeg)
                    df_eeg = pd.DataFrame(list(s_eeg.items()), columns= list('ab'))
                    Anlagen_eeg = df_eeg.set_index('a').transpose()                
                    Anlagen_eeg.to_csv(g,  header=False, sep=';', index=False, encoding='utf-8')  
                except:
                    pd.DataFrame({'EegMastrNummer': [EegMastrNummer]}).to_csv(eg,  header=False, sep=';', index=False, encoding='utf-8')
                    pass
        
            if ('Lokation' in GetOperator or GetOperatorList == None) and Einheittyp_name[Einheittyp] in list_lokation:
                try:
                    LokationMastrNummer = Einheit.LokationMastrNummer.values[0]
                    c_lok = Einheittyp_LOKATIONoperation[Einheittyp](apiKey=apiKey, marktakteurMastrNummer=myMastrNr, lokationMastrNummer=LokationMastrNummer)
                    s_lok = serialize_object(c_lok)
                    df_lok = pd.DataFrame(list(s_lok.items()), columns= list('ab'))
                    Anlagen_lok = df_lok.set_index('a').transpose()                
                    Anlagen_lok.to_csv(l,  header=False, sep=';', index=False, encoding='utf-8') 
                except:
                    pd.DataFrame({'LokationMastrNummer': [LokationMastrNummer]}).to_csv(el,  header=False, sep=';', index=False, encoding='utf-8')
                    pass   
                
            if ('AnlageKwk' in GetOperator or GetOperatorList == None) and Einheittyp_name[Einheittyp] in list_kwk:
                try:
                    KwkMastrNummer = Einheit.KwkMastrNummer.values[0]
                    c_kwk = Einheittyp_KWKoperation[Einheittyp](apiKey=apiKey, marktakteurMastrNummer=myMastrNr, kwkMastrNummer=KwkMastrNummer)
                    s_kwk = serialize_object(c_kwk)
                    df_kwk = pd.DataFrame(list(s_kwk.items()), columns= list('ab'))
                    Anlagen_kwk = df_kwk.set_index('a').transpose()                
                    Anlagen_kwk.to_csv(k,  header=False, sep=';', index=False, encoding='utf-8') 
                except:
                    pd.DataFrame({'KwkMastrNummer': [KwkMastrNummer]}).to_csv(ek,  header=False, sep=';', index=False, encoding='utf-8')
                    pass       

#deleting all files of size 0
    for dirpath, dirs, files in os.walk(path_Einheiten_folder):
        for file in files: 
            path = os.path.join(dirpath, file)
            if os.stat(path).st_size == 0:
                os.remove(path) 
                
    return None
    




