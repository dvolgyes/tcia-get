#!/usr/bin/python3
# -*- coding: future_fstrings -*-

from collections import defaultdict
from contracts import contract, new_contract
import requests
# ~import pydicom as dcm
import numpy as np
import json
import sys
import fnmatch
import signal
from optparse import OptionParser
import os
from contextlib import contextmanager
import itertools

# ~from prettyprinter import cpprint as pprint

__version__ = '0.1.0'
__title__ = 'tcia_get'
__summary__ = 'tcia_get - a downloader for TCIA collections'
__uri__ = 'https://gitlab.com/dvolgyes/tcia_get'
__license__ = 'AGPL v3'
__author__ = 'David Völgyes'
__email__ = 'david.volgyes@ieee.org'
__doi__ = 'N/A'


RESOURCES = frozenset({'TCIA', 'SharedList'})

QUERIES = {'TCIA': frozenset({'getCollectionValues',
                              'getModalityValues',
                              'getBodyPartValues',
                              'getCollectionValues',
                              'getModalityValues',
                              'getBodyPartValues',
                              'getManufacturerValues',
                              'getPatient',
                              'PatientsByModality',
                              'getPatientStudy',
                              'getSeries',
                              'getSeriesSize',
                              'getImage',
                              'NewPatientsInCollection',
                              'NewStudiesInPatientCollection',
                              'getSOPInstanceUIDs',
                              'getSingleImage'}),
           'SharedList': frozenset({'ContentsByName'})}

PARAMS = frozenset({'Modality',
                    'Collection',
                    'Date',
                    'name',
                    'BodyPartExamined',
                    'Manufacturer',
                    'PatientID',
                    'ManufacturerModelName',
                    'SOPInstanceUID',
                    'SeriesInstanceUID',
                    'StudyInstanceUID'})

FORMATS = frozenset({'CSV', 'HTML', 'XML', 'JSON', 'ZIP', 'DICOM'})

new_contract('param', lambda x: set(x.keys()).issubset(PARAMS))
new_contract('resource', lambda x: x in RESOURCES)
new_contract('tcia_query', lambda x: x in QUERIES['TCIA'])
new_contract('sharedlist_query', lambda x: x in QUERIES['SharedList'])
new_contract('format', lambda x: x in FORMATS)


def _freeze(param):
    if type(param) in [set, frozenset]:
        return frozenset(param)
    if type(param) in [dict, defaultdict]:
        for key in param:
            param[key] = _freeze(param[key])
        return param
    assert(False)

def getResponseString(response):
    if response.getcode() is not 200:
        raise ValueError("Server returned an error")
    else:
        return response.read()

def join_dicts(a,b,key):
    for x,y in itertools.product(a,b):
        if x.get(key)==y.get(key):
            z = x.copy() 
            z.update(y)
            yield z


class TCIA():

    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self.URL = 'https://services.cancerimagingarchive.net/services/v3'
        self.params = {
            'getCollectionValues': frozenset(),
            'getBodyPartValues': {'Collection', 'Modality'},
            'getModalityValues': {'Collection', 'BodyPartExamined'},
            'getManufacturerValues': {'Collection', 'Modality', 'BodyPartExamined'},
            'getPatient': {'Collection'},
            'PatientsByModality': {'Collection', 'Modality'},
            'getPatientStudy': {'Collection','PatientID','StudyInstanceUID'},
            'getSeries': {'Collection', 'StudyInstanceUID',
                          'PatientID', 'SeriesInstanceUID',
                          'Modality',  'BodyPartExamined',
                          'ManufacturerModelName', 'Manufacturer'},
            'getSeriesSize': {'SeriesInstanceUID'},
            'getImage': {'SeriesInstanceUID'},
            'NewPatientsInCollection': {'Date', 'Collection'},
            'NewStudiesInPatientCollection': {'PatientID', 'Date', 'Collection'},
            'getSOPInstanceUIDs': {'SeriesInstanceUID'},
            'getSingleImage': {'SeriesInstanceUID', 'SOPInstanceUID'},
            'ContentsByName': {'name', 'SOPInstanceUID'},
        }

        self.r_params = {
            'PatientsByModality': {'Collection', 'Modality'},
            'getSeriesSize': {'SeriesInstanceUID'},
            'getImage': {'SeriesInstanceUID'},
            'NewPatientsInCollection': {'Date', 'Collection'},
            'NewStudiesInPatientCollection': {'Date', 'Collection'},
            'getSOPInstanceUIDs': {'SeriesInstanceUID'},
            'getSingleImage': {'SeriesInstanceUID', 'SOPInstanceUID'},
            'ContentsByName': {'name', 'SOPInstanceUID'},
        }

        self.format = defaultdict(lambda: frozenset({'CSV', 'HTML', 'XML', 'JSON'}))
        self.format['getImage'] = frozenset({'ZIP'})
        self.format['getSingleImage'] = frozenset({'DICOM'})
        self.format['ContentsByName'] = frozenset({'JSON'})

        self.params = _freeze(self.params)
        self.r_params = _freeze(self.r_params)
        self.r_params = defaultdict(frozenset, self.r_params)
        self.format = _freeze(self.format)


        self.session = requests.session()
        self.session.headers.update({'API_KEY':self.API_KEY})

    def test_definitions(self):
        for key in self.params:
            assert key in (QUERIES['TCIA'] | QUERIES['SharedList'])
            assert self.params[key].issubset(PARAMS)
        for key in self.r_params:
            assert key in (QUERIES['TCIA'] | QUERIES['SharedList'])
            assert self.r_params[key].issubset(PARAMS)
        for key in self.format:
            assert self.format[key].issubset(FORMATS)

    @contract(res='resource',
              endpoint='tcia_query|sharedlist_query',
              params='param',
              fmt='format')
    def query(self, res, endpoint, params, fmt='JSON', debug=False):
        if endpoint not in QUERIES[res]:
            raise ValueError
        paramkeys = set(params.keys())
        if not paramkeys.issubset(self.params[endpoint]):
            raise ValueError
        if not self.r_params[endpoint].issubset(paramkeys):
            raise ValueError
        if fmt not in self.format[endpoint]:
            raise ValueError

        url = "{}/{}/query/{}".format(self.URL,res,endpoint)
        parameters=params.copy()
        parameters['format']=fmt
        if debug:
            print(url)
            print(parameters)
        r = self.session.get(url, params=parameters)
        if fmt=='JSON':
            return json.loads(r.text)
        return r.content

    def get_collections(self):
        collections = self.query('TCIA','getCollectionValues',{},'JSON')
        for item in collections:
            yield item['Collection']


    def get_modality_values(self,collection):
        modality = self.query('TCIA','getModalityValues',{'Collection':collection},'JSON')
        yield modality

    def get_series_size(self,seriesUID):
        size = self.query('TCIA','getSeriesSize',{'SeriesInstanceUID':seriesUID},'JSON')
        return size

    def get_series(self,collection):
        series = self.query('TCIA','getSeries',{'Collection':collection},'JSON')
        patients = self.get_study(collection)
        yield from join_dicts(series,patients,'StudyInstanceUID')

    def get_patient(self,collection):
        patients = self.query('TCIA','getPatientStudy',{'Collection':collection},'JSON')
        yield from patients


    def save_series(self,series,name=None):
        zipcontent = self.query('TCIA','getImage',{'SeriesInstanceUID':series['SeriesInstanceUID']},'ZIP')
        patientID = series['PatientID']
        modality = series['Modality']
        seriesID = series['SeriesInstanceUID']
        if name is None:
            name = f'{patientID}_{modality}_{seriesID}.zip'.format(series)
        with open(name,'wb+') as f:
            f.write(zipcontent)
        return name

    def get_study(self,collection):
        yield from self.query('TCIA','getPatientStudy',{'Collection':collection},'JSON')


@contract(istr='string', pattern='string|list|None')
def _pattern_match(istr, pattern):
    if isinstance(pattern,list):
        for p in pattern:
            if _pattern_match(istr,p):
                return True
        return False
    if pattern is None:
        return True
    s = istr.lower()
    p = pattern.lower()
    if s.find(p)>=0:
        return True
    if fnmatch.fnmatch(s,p):
        return True
    return False


@contract(pattern='list')
def _search(pattern=None, url_print=True,tcia=None):
    collections = list(tcia.get_collections())
    maxlen = max(len(x) for x in collections)+2
    for co in collections:
        if not _pattern_match(co,pattern):
            continue
        if url_print:
            url = co.replace(" ","+")
            yield co,f'https://wiki.cancerimagingarchive.net/display/Public/{url}'
        else:
            yield co

def search(pattern=None, url_print=True, tcia=None):
    if not url_print:
        for result in _search(pattern,url_print,tcia=tcia):
            print(result)
    else:
        for co,url in _search(pattern,url_print,tcia=tcia):
            c = f'"{co}"'
            print(f'{c:{30}}  :  https://wiki.cancerimagingarchive.net/display/Public/{url}')


@contextmanager
def remember_cwd():
    cwd=os.path.abspath(os.getcwd())
    yield cwd
    os.chdir(cwd)

def download_collection(collections,tcia=None):
    with remember_cwd() as cwd:
        for collection,url in _search(collections,tcia=tcia):
            if not os.path.exists(os.path.join(cwd,collection)):
                os.mkdir(os.path.join(cwd,collection))
            os.chdir(os.path.join(cwd,collection))
            print(f'Collection:  {collection}    url: {url}')
            for series in tcia.get_series(collection):
                fname = tcia.save_series(series)
                print(f'  file: {collection}/{fname}')

if __name__ == "__main__":

    parser = OptionParser(
        usage='%prog [options] RECORD_OR_DOI', version=f'%prog {__version__}'
    )

    parser.add_option(
        '-c',
        '--cite',
        dest='cite',
        action='store_true',
        default=False,
        help='print citation information',
    )

    parser.add_option(
        '-s',
        '--search',
        dest='search',
        action='append',
        help='search records',
        default=[]
    )

    parser.add_option(
        '-k',
        '--key',
        dest='key',
        action='store',
        help='API key',
        default=None
    )

    parser.add_option(
        '-d',
        '--download',
        action='append',
        dest='collection',
        help='download collection(s)',
        default=[]
    )

    (options, args) = parser.parse_args()


    if options.key is None:
        print('API key is mandatory!')
        sys.exit(1)

    tcia=TCIA(options.key)

    if len(options.search)>0:
        search(options.search,tcia=tcia)
    elif len(options.collection)>0:
        download_collection(options.collection,tcia=tcia)
    else:
        parser.print_help()

