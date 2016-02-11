#!/usr/bin/env python2.7

# Copyright 2016 Jake Bouma.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an.
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific.
# language governing permissions and limitations under the License.


from flask import stream_with_context, request, Response
from flask import Flask
from time import sleep
import sys
from pyrfc import Connection as SAP_cnxn
import pandas as pd
import sqlalchemy
import json
from datetime import datetime


app = Flask(__name__)

dd03l_fields = ['FIELDNAME', 'AS4LOCAL', 'AS4VERS', 'POSITION',
                'KEYFLAG', 'ROLLNAME', 'CHECKTABLE', 'INTTYPE',
                'INTLEN', 'LENG']


def gFIELDS(fields):
    return [{'FIELDNAME': fi} for fi in fields]


@app.route('/')
def hello_world():
    sleep(0.5)
    return 'UP'


@app.route('/meta', methods=['POST'])
def app_get_meta():
    """
    Function wrapping for get_meta.
    :return: Response with json data:
              (1) 'meta_csv' a dataframe written to csv and
              (2) 'vchunks' a list of field groupings that can be downloaded
                   within the sap_buffer_size limit.
    """
    print(request.method)
    resp = get_meta(**json.loads(request.data))
    return Response(json.dumps(resp))


def get_meta(cnxn_details, table_name, fields=None, sap_buffer_size=400):
    """
    Connects to SAP system and downloads table metadata.
    :param cnxn_details: The SAP system's connection details
    :param table_name: The SAP table to fetch metadata for
    :param fields: (optional) Only fetch a field subset of the metadata
    :param sap_buffer_size: (optional) The column-wise buffersize that
           the download RFC, BBP_RFC_READ_TABLE, uses.
    :return: metadata; a dictionary containing
             (1) 'meta_csv' a dataframe written to csv and
             (2) 'vchunks' a list of field groupings that can be downloaded
                 within the sap_buffer_size limit.
    """
    # Fetch metadata from SAP data dictionary table
    with SAP_cnxn(**cnxn_details) as cnxn:
        meta_result = cnxn.call('BBP_RFC_READ_TABLE',
                                QUERY_TABLE='DD03L',
                                DELIMITER='|',
                                OPTIONS=[{'TEXT': "TABNAME = '%s'"
                                         % table_name}],
                                FIELDS=gFIELDS(dd03l_fields))
        data = [map(unicode.strip, x['WA'].split('|'))
                for x in meta_result['DATA']]
        columns = [x['FIELDNAME'] for x in meta_result['FIELDS']]

    # Build a dataframe for convenience
    meta = pd.DataFrame(data=data, columns=columns).set_index('FIELDNAME')
    meta.LENG = meta.LENG.astype(int)
    meta = meta[meta.LENG > 0]  # drop .INCLUDE etc

    # Will pass data back as csv
    meta_csv = meta.to_csv(encoding='utf-8')

    # Count vchunks; determine column-wise chunks with sap_buffer_size
    if fields is None:
        fields = meta.index.tolist()
    vchunks = []
    chunksum = 0
    chunk = []
    for field in fields:
        length = meta.loc[field.upper(), 'LENG']
        chunksum += length
        if chunksum > sap_buffer_size:
            vchunks.append(chunk)
            chunksum = 0
            chunk = [field]
        else:
            chunk.append(field)
    if chunk:
        vchunks.append(chunk)
    vchunks = vchunks

    # Return
    metadata = {'meta_csv': meta_csv, 'vchunks': vchunks}
    return metadata


@app.route('/read', methods=['POST'])
def app_read():
    """
    Function wrapping for read.
    :return: A response with 'metacsv' and 'vchunks' in the json data.
    """
    return Response(read(**json.loads(request.data)))


def read(cnxn_details, table_name, vchunks, ri, n, where,
         sqlalchemy_cnxnstr='sqlite:////home/cks/db.sqlite',
         output_tablename=None, keep=False):
    """

    :param cnxn_details:
    :param table_name:
    :param vchunks:
    :param ri:
    :param n:
    :param where:
    :param sqlalchemy_cnxnstr:
    :param output_tablename:
    :param keep:
    :return:
    """
    with SAP_cnxn(**cnxn_details) as cnxn:
        data = None
        fields = None

        # For this row-wise chunk, loop through column-wise chunks
        for vchunk in vchunks:
            chunk_response = cnxn.call(
                'BBP_RFC_READ_TABLE', QUERY_TABLE=table_name,
                DELIMITER='|', OPTIONS=[{'TEXT': where}],
                FIELDS=gFIELDS(vchunk), ROWCOUNT=n,
                ROWSKIPS=ri, NO_DATA='')
            if chunk_response['DATA'] is None:
                # There are no rows...
                break

            # parse
            chunk = [map(unicode.strip, x['WA'].split('|'))
                     for x in chunk_response['DATA']]
            if data is None:
                # First pass, initialise
                data = chunk
                fields = vchunk
            else:
                # Subsequent passes, append columns
                data = [data[idx] + chunkrow
                        for idx, chunkrow in enumerate(chunk)]
                fields += vchunk

        # Use a dataframe for convenience
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = pd.DataFrame(data=data, columns=fields)
        df['TIMESTAMP'] = timestamp
        count = len(df)

        # write to a database
        json_out = {'STATUS': 'FAIL', 'TIMESTAMP': timestamp, 'COUNT': count}
        if sqlalchemy_cnxnstr is not None:
            engine = sqlalchemy.create_engine(sqlalchemy_cnxnstr)
            output_tablename = output_tablename if output_tablename else table_name
            df.to_sql(output_tablename,
                      engine, if_exists='append',
                      chunksize=50000, index=False)
            json_out['STATUS'] = 'OK'
        # return the data
        if keep:
            json_out['DATA'] = df.to_csv(index=False, encoding='utf-8')
            json_out['STATUS'] = 'OK'
        return json.dumps(json_out)


@app.route('/info', methods=['POST', 'GET'])
def info():
    """
    Checks the table's connection to SAP.
    cnxn_details must be provided in the request.
    :return: Response, json data:
               - 'status' <'OK'|'fail'>,
               - 'data' <dict of connection attributes>
    """
    reqdata = json.loads(request.data)
    cnxn_details = reqdata['cnxn_details']
    try:
        with SAP_cnxn(**cnxn_details) as cnxn:
            cnxn.ping()
            cnxn_info = cnxn.get_connection_attributes()
            test = cnxn.call('STFC_CONNECTION', REQUTEXT='Connection Test')
            cnxn.close()
        return json.dumps({'status': 'OK', 'data': cnxn_info})
    except Exception, e:
        resp = json.dumps({'status': 'fail', 'data': str(e)})
    return Response(resp)


# Run the app
if __name__ == '__main__':
    app.debug = True
    try:
        port = int(sys.argv[1])
    except IndexError:
        port = 5101
    app.run(host='0.0.0.0', port=port)

