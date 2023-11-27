import sender
import simple_tools
from data_modifier import *
import names
import argparse


def main():

    parser = argparse.ArgumentParser(description="Program for handling various command line arguments.")
    # Adding arguments with flags
    parser.add_argument('--company', action=argparse.BooleanOptionalAction)
    parser.add_argument('--sheet', type=str, help='Name of the sheet')
    # parser.add_argument('--publish', action=argparse.BooleanOptionalAction)
    parser.add_argument('--all_row', action=argparse.BooleanOptionalAction)
    parser.add_argument('--private', action=argparse.BooleanOptionalAction)
    parser.add_argument('--header', type=int, help='First row index - header')
    parser.add_argument('--start_row', type=int, help='Start row')
    parser.add_argument('--end_row', type=int, help='End row')
    parser.add_argument('--multiplication', type=int, help='Multiplicate dataset')
    parser.add_argument('--batch', action=argparse.BooleanOptionalAction)
    parser.add_argument('--output_file', type=str, help='Name of output file')
    parser.add_argument('--publishers', type=int, help='Number of publishers')
    parser.add_argument('--documents', type=int, help='Documents per publishers')
    parser.add_argument('--get_urls', action=argparse.BooleanOptionalAction)
    parser.add_argument('--random_docs', action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    # Parsing arguments
    try:
        if args.publishers is not None:
            amount_of_publishers = args.publishers
        else:
            amount_of_publishers = None
        if args.documents is not None:
            amount_of_documents = args.documents
        else:
            amount_of_documents = None
        name_of_prepared = args.output_file
        if args.company is not None:
            is_company = args.company
        else:
            is_company = False
        sheet = args.sheet
        all_row = args.all_row
        if not all_row:
            start_row = args.start_row
            end_row = args.end_row
        else:
            start_row = 0
            end_row = 0
        if args.header is not None:
            header = args.header
        else:
            header = 0
        if args.multiplication is not None:
            multiplication = args.multiplication
        else:
            multiplication = 0
        if args.private is None:
            private = False
        else:
            private = args.private
        if args.batch is None:
            batch = False
        else:
            batch = args.batch
        if args.get_urls is not None:
            get_urls = args.get_urls
        else:
            get_urls = False
        if args.random_docs is not None:
            random_docs = args.random_docs
        else:
            random_docs = False
    except:
        print("WRONG ARGUMENTS")
        exit()
    if not get_urls:
        excel_file = 'v6 Battery Dataset -finalizedv5.xlsm'
    else:
        excel_file = 'v7 Battery Dataset -finalizedv5.xlsm'
    print("Loading Data From: "+sheet)
    result_json = excel_to_json(excel_file, sheet, start_row, end_row, _all=all_row, header=header)
    if not is_company:
        output_json_file = "DataSetsJSON/"+sheet + ".json"
    else:
        output_json_file = "Companies/"+sheet+".json"

    with open(output_json_file, 'w') as f:
        json.dump(result_json, f, indent=4)

    if is_company:
        return  # Exit if the "is_company" argument is true
    const_url = None
    urls_to_skip = []
    if batch:
        batch_document = []
    if amount_of_publishers is not None:
        publishers_count = {}
    else:
        publishers_count = None
    first = True
    for result in result_json:
        title = create_title(result, sheet)
        try:
            company = get_company_for_product(result).rstrip().strip()
        except:
            print("Failed to get company for product")
            company = None
        if not get_urls:
            url = get_company_url(company)
        else:
            url = get_url_from_sheet(result)
        if url is None:
            if const_url is None:
                default_publisher = get_default_url_from_config()
                if default_publisher in urls_to_skip:
                    continue
                u_input = input("URL not found publish on default publisher? "
                                "\n y - yes"
                                "\nn - no, don't publish"
                                "\na - yes, always in this run"
                                "\n: ")
                if u_input == "n":
                    return
                if u_input == "y":
                    url = default_publisher
                elif u_input == "a":
                    const_url = default_publisher
                    url = default_publisher
            else:
                if url in urls_to_skip:
                    continue
                url = const_url
        if publishers_count is not None and url in publishers_count:
            if publishers_count[url] >= amount_of_documents:
                const_url = None
                urls_to_skip.append(url)
                if len(publishers_count) >= amount_of_publishers:
                    if simple_tools.check_publishers_in_dict(publishers_count, amount_of_documents):
                        break
                continue
            publishers_count[url] += 1
        elif publishers_count is not None and url not in publishers_count:
            if len(publishers_count) >= amount_of_publishers:
                if simple_tools.check_publishers_in_dict(publishers_count, amount_of_documents):
                    break
                continue
            else:
                publishers_count[url] = 1

        if private:
            receiver_url = get_receiver_url(result, sheet)
            cif = get_cif(result, sheet)
        # CREATE ADDITIONAL DETAILS
        additional_details_dict = create_additional_details(result, sheet)
        additional_details = json.dumps(additional_details_dict, ensure_ascii=False)
        additional_details = str(additional_details).replace("None", "null")

        # GET CATEGORY AND DOCUMENT PATH
        if random_docs:
            document_path = []
        else:
            document_path = get_document_path(result, sheet)

        if len(title) > len(document_path) > 0:
            fix_title = []
            for i in range(len(document_path)):
                fix_title.append(title[i])
            title = fix_title

        category = names.categories[sheet]
        try:
            product_id = additional_details_dict["productId"]
        except:
            product_id = ""
        print(f"LOADED DATA: \ntitle: {title}\ncategory: {category}\nadditional details: {additional_details}\nurl: {url}\ndocuments paths: {document_path}")

        if private:
            sender.prepare_as_private(sheet,
                                      url,
                                      category,
                                      title,
                                      additional_details,
                                      name_of_prepared,
                                      product_id,
                                      document_path,
                                      cif=cif,
                                      receiver_url=receiver_url)
        else:
            sender.prepare_as_public(
                                     url,
                                     category,
                                     title,
                                     additional_details,
                                     name_of_prepared,
                                     product_id,
                                     document_path, first=first, generate=False)
        for _ in range(multiplication):
            additional_details_dict["productId"] = str(_)+"-"+product_id
            additional_details = json.dumps(additional_details_dict, ensure_ascii=False)
            additional_details = str(additional_details).replace("None", "null")
            if not private:
                sender.prepare_as_public(
                                         url,
                                         category,
                                         title,
                                         additional_details,
                                         name_of_prepared,
                                         product_id,
                                         document_path)
            else:
                sender.prepare_as_private(sheet,
                                          url,
                                          category,
                                          title,
                                          additional_details,
                                          name_of_prepared,
                                          product_id,
                                          document_path,
                                          cif="some_blockchain_id",
                                          receiver_url="some_url")
        first = False
    print(f"Data saved to: {output_json_file}")


if __name__ == "__main__":
    main()
