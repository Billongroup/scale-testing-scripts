import copy
import yaml
import sender
import datetime
import time
import json
import argparse
import generatePDF
import os
import uuid
import sys


def to_json(_list):
    try:
        json_data = json.dumps(_list)
        return json_data
    except Exception as e:
        return str(e)


def multiplicate(file_name, level, pdf_name):
    if level == 0:
        return
    prepared_file_name = "Prepared/" + file_name
    print(f"MULTIPLICATION STARTED FOR {prepared_file_name}")
    # Otwórz plik do odczytu
    with open(prepared_file_name, 'r') as file:
        data = yaml.safe_load(file)

    new_data_list = []

    for x in range(1, level+1):
        print(f"MULTIPLICATION {prepared_file_name}: {str(x)}")
        external_id = []
        new_ad = []

        for d in json.loads(data[0]['additional_details']):
            d_copy = copy.deepcopy(d)
            d_copy_ad = json.loads(d_copy['additional_details'])
            d_copy_ad['productId'] = f"{str(x)}-" + str(d_copy_ad['productId'])
            external_id.append(d_copy_ad['productId'])
            d_copy['additional_details'] = json.dumps(d_copy_ad)
            new_ad.append(d_copy['additional_details'])

        catalog, name_of_doc = os.path.split(data[0]["source_documents"][0])
        new_name = f'{str(x)}-{str(file_name.replace(".yaml",""))}-' + name_of_doc
        new_document = os.path.join(catalog, new_name)

        new_data_list.append({
            "additional_details": json.dumps(new_ad),
            "batching_level": data[0]["batching_level"],
            "category": data[0]["category"],
            "external_id": str(external_id),
            "is_private": data[0]["is_private"],
           # "source_documents": [pdf_name],
            "title": [f'{str(x)}-' + data[0]["title"][0]],
            "url": data[0]["url"]
        })

        #generatePDF.generate_given(new_document)

    # Otwórz plik do zapisu w trybie 'a' (append) i zapisz wszystkie nowe dane naraz
    with open(prepared_file_name, 'a', buffering=4096) as file:
        yaml.dump_all([new_data_list], file)
    print(f"MULTIPLICATION DONE FOR {prepared_file_name}")
    return True


def main(data_loaded, multi_level, server_join=False, outputname=None, lock_level=0):

    additional_details = []
    unique_id = str(uuid.uuid4())
    title = "Batch_" + unique_id
    category = data_loaded[0]['category']
    url = data_loaded[0]['url']
    print(f"BATCHING FOR {url}")
    product_id = []
    other_data = []
    sp_other_data = []
    batching_level = 0
    if lock_level > 0:
        if server_join:
            for data in data_loaded:
                if data['url'].split("//")[-1].split(":")[0] == url.split("//")[-1].split(":")[0]:
                    if batching_level >= lock_level:
                        sp_other_data.append(data)
                    else:
                        additional_details.append(data)
                        product_id.append(data["external_id"])
                        batching_level += 1
                else:
                    other_data.append(data)
        else:
            for data in data_loaded:
                if data['url'] == url:
                    if batching_level >= lock_level:
                        sp_other_data.append(data)
                    else:
                        additional_details.append(data)
                        product_id.append(data["external_id"])
                        batching_level += 1
                else:
                    other_data.append(data)
    else:
        if server_join:
            for data in data_loaded:
                if data['url'].split("//")[-1].split(":")[0] == url.split("//")[-1].split(":")[0]:
                    additional_details.append(data)
                    product_id.append(data["external_id"])
                    batching_level += 1
                else:
                    other_data.append(data)
        else:
            for data in data_loaded:
                if data['url'] == url:
                    additional_details.append(data)
                    product_id.append(data["external_id"])
                    batching_level += 1
                else:
                    other_data.append(data)
    product_id = to_json(product_id)
    additional_details = to_json(additional_details)
    unique_id = str(uuid.uuid4())
    if outputname is None:
        outputname = f'{url.replace("http://","")}_BATCH_{unique_id}_{filename}'
    pdf_name = f'Documents/{url.replace("http://","")}.pdf'
    sender.prepare_as_public(url,
                             category,
                             title,
                             str(additional_details),
                             outputname,
                             str(product_id),
                             batching_level=batching_level,
                             generate_given=pdf_name)

    if sp_other_data:
        main(sp_other_data, 0, server_join=server_join, lock_level=lock_level, outputname=outputname)
    m_done = multiplicate(outputname, level=multi_level, pdf_name=pdf_name)
    if m_done:
        print("Multiplication Done")
    if other_data:
        main(other_data, multi_level, server_join=server_join, lock_level=lock_level)
    print(f"BATCHING {url} DONE")
    return outputname


if __name__ == "__main__":
    sys.setrecursionlimit(100000)
    parser = argparse.ArgumentParser(description="Program for handling various command line arguments.")
    # Adding arguments with flags
    parser.add_argument('--multiplication', type=int, help='multiplicate batch')
    parser.add_argument('--dataset', type=str, help='filename of data')
    parser.add_argument('--server_batch', action=argparse.BooleanOptionalAction)
    parser.add_argument('--batching_level', type=int, help='amount of documents in one batch')
    args = parser.parse_args()
    if args.multiplication is not None:
        multiplication = args.multiplication
    else:
        multiplication = 0
    if args.dataset is not None:
        dir_filename = args.dataset
    else:
        print("No dataset given")
        exit()
    if args.server_batch is not None:
        join_servers = args.server_batch
    else:
        join_servers = False
    if args.batching_level is not None:
        lock_level = args.batching_level
    else:
        lock_level = 0
    index_slash = dir_filename.find("/")
    if index_slash != -1:
        filename = dir_filename[index_slash + 1:]
    else:
        print("WRONG FILE NAME, USE Prepared FOLDER")
        exit()

    with open(dir_filename, 'r', encoding='utf-8') as stream:
        loaded = yaml.safe_load(stream)
    main(loaded, multiplication, server_join=join_servers, lock_level=lock_level)
