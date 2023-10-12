# 2023-10-12
import argparse
import importlib
import re
import time
import glob
import yaml
from collections import defaultdict
from functools import partial


class Config:
    def __init__(self, pdf_dir='./pdfs'):
        self.pubs = {}
        # Documents to publish per publisher
        self.documents_to_publish = 3
        self.documents_to_publish_for_pub = {}
        self.sizeKB = 500
        # Max publications in progress per publisher
        self.max_queue_size = 20
        # Min publications in progress per publisher
        self.min_queue_size = 1
        self.send_delay = 0
        self.identitiesFilename = 'publishers.csv'
        self.use_predefined_pdfs = False
        self.pdf_dir = pdf_dir

        self.ACC = 3

        self.debug9000 = False
        self.loglevel = 'INFO'
        self.verbose = False
        self.read_after = False
        self.write_on_disk = False
        self.csv_file = None
        self.read_only = False
        self.update = False
        self.update_immediate = 0

        self.PUBLISH_TIMEOUT_S = 900
        self.SLEEP_AFTER_CHECK = 6

        self.threads_per_publisher = 1
        self.early_finish = False

        self.private = False
        self.private_for_publisher = False
        self.action = 'run'
        self.timeout = None
        self.rcv_publishers = None
        self.test_duration = 0
        self.pdf_file = None
        self.pdf_getter = None
        self.pubs_instruction = {}
        self.pubs_categories = {}

    def getTime(self):
        return round(time.time(), self.ACC)

    def readPubsFromColonyConfig(self, config, publishersLimit=None):
        spec = importlib.util.spec_from_file_location("module.name", config)
        if spec:
            # this will work when module is given as path or filename
            c = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(c)
        else:
            # this will work when config given as name (without extension)
            config = config.replace('/', '.')
            c = importlib.import_module(config)
        self.pubs = {}
        publishersLimit = publishersLimit or 999999
        numPublishers = 0
        for serverConf in c.servers_conf:
            host = serverConf['host']
            ports = []
            for node in serverConf['nodes']:
                if 'user' not in node:
                    continue
                if 'PUBLISHER' not in node['user']:
                    continue
                extra = node['extra_params']
                PATT = re.compile(r'.*--durmedport=(\d+)')
                m = PATT.match(extra)
                if not m:
                    print('strange, found publisher without port: ' + node['user'] + '  ' + extra)
                    continue
                ports.append(int(m.groups(1)[0]))
                numPublishers += 1
                if numPublishers >= publishersLimit:
                    break
            if ports:
                self.pubs[host] = ports
            if numPublishers >= publishersLimit:
                break

    def findPubsOnColony(self, borgUtils, publishersLimit=None, group_id=None, sub_name='PUBLISHER'):
        publishersLimit = publishersLimit or 999999
        numPublishers = 0
        self.pubs = {}
        for server in borgUtils.get_colony_servers():
            host = server['host']
            ports = []
            nodes = borgUtils.update_nodes_info_master(servers_list=[server])
            for node in nodes:
                if not hasattr(node, 'group_type') or node.group_type != 'GROUPPUBLISHER':
                    continue
                if node.user is None or sub_name not in node.user:
                    continue
                if group_id is not None and node.group_id != group_id:
                    continue
                port = node.get_parameter('durmedport')
                if port is None:
                    print('strange, found publisher without port: ' + node.user + '  ' + node.parameters)
                    continue
                ports.append(int(port))
                numPublishers += 1
                if numPublishers >= publishersLimit:
                    break
            if ports:
                self.pubs[host] = ports
            if numPublishers >= publishersLimit:
                break

    def input_file_analyser(self, input_file):
        with open(input_file, 'r', encoding='utf-8') as stream:
            data_loaded = yaml.safe_load(stream)
        for record in data_loaded:
            url = record['url']
            ip, port = url.split('/')[-1].split(':')
            addr = ip + ':' + str(port)
            if ip not in self.pubs.keys():
                self.pubs[ip] = []
            if port not in self.pubs[ip]:
                self.pubs[ip].append(port)
                self.documents_to_publish_for_pub[addr] = 1 # nie sa uwzglednione update, ale to dobrze
            else:
                self.documents_to_publish_for_pub[addr] += 1
            if url not in self.pubs_instruction:
                self.pubs_instruction[url] = []
                self.pubs_categories[url] = set()
            del record['url']
            self.pubs_instruction[url].append(record)
            self.pubs_categories[url].add(record['category'])

    def precise_pdf_getter(self, pub):
        for r in self.pubs_instruction[pub]:
            yield r['source_documents'], r['additional_details'], r['category'], r['title'], r.get('blockchain_id', None), r.get('receiver_url', None)
        while True:
            yield None, None, None, None, None, None

    def default_pdf_getter(self, pub):
        additional_details = f'Here be PUBLIC additional details for {pub}'
        documentMainCategory = 'ROOT'
        cif = None
        receiver_url = None
        title = None
        print(glob.glob(f'{self.pdf_dir}/*pdf'))
        for pdf in glob.glob(f'{self.pdf_dir}/*pdf'):
            yield [pdf], additional_details, documentMainCategory, [title], cif, receiver_url
        while True:
            yield [], additional_details, documentMainCategory, [title], cif, receiver_url

    def readConfFromArgparse(self, params):

        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('action', help='Action to execute', choices=['setup', 'categories', 'run', 'noop'], nargs='?', default=self.action)
        parser.add_argument('-c', '--configFile', help='Path to config.py of colony')
        parser.add_argument('--publishers', help='Override publishers config')
        parser.add_argument('--publishers_limit', help='Only select a few first publishers from config', type=int)
        parser.add_argument('--identities', action='store', type=str, help='Name of the file with identities list.')
        parser.add_argument('--input_file', action='store', type=str, help='Name of the yaml file with publication instruction.')
        parser.add_argument('-n', '--num_publications', help='How many documents per publisher will be published', type=int, default=self.documents_to_publish)
        parser.add_argument('-s', '--size', help='Size of documents to publish [kB]', type=int, default=self.sizeKB)
        parser.add_argument('-q', '--queue_size', help='Max number of concurrent publications', type=int, default=self.max_queue_size)
        parser.add_argument('-m', '--min_queue_size', help='Min number of concurrent publications', type=int, default=self.min_queue_size)
        parser.add_argument('-t', '--timeout', help='Number of seconds before finishing with failure', type=int, default=self.timeout)
        parser.add_argument('-d', '--send_delay', help='Delay between two sends in seconds', type=float, default=self.send_delay)
        parser.add_argument('--private', help='Execute private docs publishing', action='store_true', default=self.private)
        parser.add_argument('--private_for_publisher', help='Execute private docs publishing, available to read by other publisher', action='store_true', default=self.private)
        parser.add_argument('--threads', help='Number of threads per publisher.', action='store', type=int, default=self.threads_per_publisher)
        parser.add_argument('--early_finish', help='finish when first publisher finished publishing all of his documents', action='store_true', default=self.early_finish)
        parser.add_argument('--loglevel', help='log level INFO by default', type=str, default=self.loglevel)
        parser.add_argument('-v', '--verbose', help='verbose output on console', action='store_true', default=self.verbose)
        parser.add_argument('--read_after', help='Reads document after successful publishing', action='store_true', default=self.read_after)
        parser.add_argument('--write_on_disk', help='Reads document after successful publishing', action='store_true', default=self.read_after)
        parser.add_argument('--read_only', help='Reads all documents from provided csv file', default=None)
        parser.add_argument('--update', help='Updates all documents provided in csv file', default=None)
        parser.add_argument('--update_immediate', help='Updates documents right after publish', type=int, default = 0)
        parser.add_argument('--test_duration', help='Test duration in seconds', type=int, default = 0)
        parser.add_argument('--rcv_publishers', help='Publishers receiving private docs')
        parser.add_argument('--pdf_file', help='Path to pdf to publish', default=None)
        parser.add_argument('--use_predefined_pdfs', action='store_true', default=False, help='Use predefined pdf from ./pdfs directory')


        args = parser.parse_args(params)

        # there is no nice way to set dest as separate structure
        self.documents_to_publish = args.num_publications
        self.sizeKB = args.size
        self.max_queue_size = args.queue_size
        self.min_queue_size = args.min_queue_size
        self.timeout = args.timeout
        self.send_delay = args.send_delay
        self.private = args.private
        self.private_for_publisher = args.private_for_publisher
        self.threads_per_publisher = args.threads
        self.early_finish = args.early_finish
        self.loglevel = args.loglevel
        self.verbose = args.verbose
        self.read_after = args.read_after
        self.write_on_disk = args.write_on_disk
        self.update_immediate = args.update_immediate
        self.test_duration = args.test_duration
        self.pdf_file = args.pdf_file
        if args.read_only:
            self.read_only = True
            self.csv_file = args.read_only
        if args.update:
            self.update = True
            self.csv_file = args.update

        if args.action:
            self.action = args.action
        if args.configFile:
            self.readPubsFromColonyConfig(args.configFile, args.publishers_limit)
        if args.publishers:
            self.pubs = eval(args.publishers)
        if args.rcv_publishers:
            self.rcv_publishers = eval(args.rcv_publishers)
        if args.identities:
            self.identitiesFilename = args.identities
        if args.use_predefined_pdfs:
            self.use_predefined_pdfs = True
        if args.input_file:
            self.use_predefined_pdfs = True
            # TODO stworzenie na podstawie pliku zlownika (url_publikatora, [lista opisow publikacji])
            self.input_file = args.input_file
            self.input_file_analyser(args.input_file)
            self.pdf_getter = self.precise_pdf_getter
            # TODO sprwadzenie, czy mamy dosc dokumentow do opublikowania??
        elif self.use_predefined_pdfs:
            if len(glob.glob(f'{self.pdf_dir}/*pdf')) < self.documents_to_publish * sum(list(len(self.pubs[i]) for i in self.pubs)):
                print(
                    f'Cannot publish {self.documents_to_publish * sum(list(len(self.pubs[i]) for i in self.pubs))} documents, number of available pdf files: {len(glob.glob(f"{self.pdf_dir}/*pdf"))}')
                self.documents_to_publish = len(glob.glob(f'{self.pdf_dir}/*pdf'))
            self.pdf_getter = self.default_pdf_getter
