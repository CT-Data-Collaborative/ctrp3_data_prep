import csv, os, datetime, yaml, pytz, hashlib, json
from collections import namedtuple
from functools import partial


SchemaField = namedtuple('SchemaField', 'name helper')
RangeTuple = namedtuple('RangeTuple', 'low high default')
eastern = pytz.timezone('US/Eastern')

def setup_crosswalk():
    with open('/Users/scuerda/Work/Projects/ctrp3_data_prep/raw/orgs.csv', 'r') as orgfile:
        cr = csv.DictReader(orgfile)
        with open('/Users/scuerda/Work/Projects/ctrp3_data_prep/clean/orgs_hash.csv', 'w') as outfile:
            cw = csv.DictWriter(outfile, fieldnames=['org_id', 'department_id'])
            cw.writeheader()
            for row in cr:
                hashed_name = hashlib.md5(bytes(row['department_name'], 'utf-8')).hexdigest()
                row['department_id'] = hashed_name
                del(row['department_name'])
                cw.writerow(row)

def setup_departments():
    with open('/Users/scuerda/Work/Projects/ctrp3_data_prep/raw/names.csv', 'r') as orgfile:
        cr = csv.DictReader(orgfile)
        with open('/Users/scuerda/Work/Projects/ctrp3_data_prep/clean/departments_hash.csv', 'w') as outfile:
            cw = csv.DictWriter(outfile, fieldnames=['department_id', 'department_name', 'department_type'])
            cw.writeheader()
            for row in cr:
                hashed_name = hashlib.md5(bytes(row['department_name'], 'utf-8')).hexdigest()
                row['department_id'] = hashed_name
                cw.writerow(row)




def to_int(value):
    try:
        return int(value)
    except ValueError:
        return None

def xlsdate_to_datetime(xldate, datemode=1):
    # datemode: 0 for 1900-based, 1 for 1904-based
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )

def long_date_string_to_datetime(value):
    fmt = "%m/%d/%y %H:%M"
    try:
        parsed_date = eastern.localize(datetime.datetime.strptime(value, fmt))
    except ValueError:
        try:
            fmt = "%m-%d-%y"
            parsed_date = eastern.localize(datetime.datetime.strptime(value, fmt))
        except ValueError as e:
            raise ValueError(e)
    return parsed_date

def short_date_string_to_datetime(value):
    fmt = "%B %d, %Y"
    try:
        parsed_date = eastern.localize(datetime.datetime.strptime(value, fmt))
    except ValueError:
        raise ValueError
    return parsed_date


def str_to_bool(s):
    if s.lower() == 'true':
        return True
    elif s.lower() == 'false':
        return False
    else:
        return None

def validate_range(value, range_tuple):
    """ Takes a range_tuple as an argument and validates value.

    Can also be used within a schema generation factory with functools.partial
    to build dedicated helper function with a pre-set range.
    """
    if value is None:
        return range_tuple.default
    if value >= range_tuple.low and value <= range_tuple.high:
        return value
    else:
        return range_tuple.default

def validate_unique(value, unique_set):
    """ Takes a set and a value and checks if value is in set.

    Intended to be used within a schema generation factory with functools.partial
    to build a dedicated helper function for a schema item
    """
    if value in unique_set:
        return value
    else:
        raise ValueError("Not an acceptable value!")

def helper(value, type_helper, range_helper=None, unique_helper=None, field=None):
    """
    Function to be used in concert with functools.partial to build a validation function.

    Takes a value and a type helper and validates.

    For example:
        h = functools.partial(helper, type_helper=int)

    Can also receive optional arguments for checking for unique values or checking for bounded values
    """
    try:
        final_value = type_helper(value)
    except ValueError as e:
        raise ValueError(e)
    if range_helper:
        try:
            range_helper(final_value)
        except ValueError as e:
            raise ValueError(e)
    if unique_helper:
        try:
            unique_helper(final_value)
        except ValueError as e:
            raise ValueError(e)
    return final_value

def build_helper(field):
    """ Main function for accepting a schema field and building a helper function"""
    vr = None
    vu = None
    try:
        rt = RangeTuple(field['range']['low'], field['range']['high'], field['out_of_range_value'])
        vr = partial(validate_range, range_tuple=rt)
    except KeyError:
        pass
    try:
        us = set(field['unique'])
        vu = partial(validate_unique, unique_set=us)
    except KeyError:
        pass
    input_type = field['input_type']
    if input_type == 'str':
        th = str
    elif input_type == 'int':
        th = to_int
    elif input_type == 'boolean':
        th = str_to_bool
    elif input_type == 'dateFloat' or input_type == 'dataInt':
        th = xlsdate_to_datetime
    elif input_type == 'dateStringLong':
        th = long_date_string_to_datetime
    elif input_type == 'dateStringShort':
        th = short_date_string_to_datetime
    elif input_type == 'float':
        th = float
    h = partial(helper, type_helper=th, range_helper=vr, unique_helper=vu, field=field)
    return h


# TODO: Add
class Cleaner:
    """Object for handling the cleaning and preparation of racial profiling data"""
    def __init__(self, config_file_path):
        self.config = self._load_config(config_file_path)
        self.schema = None
        self.schema_registry = {}
        self.logged = {}
        self.rows_processed = 0
        self.org_crosswalk = {}

    def _load_config(self, config_file_path):
        with open(config_file_path, 'r') as config_file:
            return yaml.load(config_file)

    def _load_schema(self):
        with open(self.config['schema_file']) as schema:
            schemas = yaml.load(schema)
            self.schema = schemas[self.config['version']]

    def _build_and_register_schema_field(self, field):
        helper = build_helper(field)
        s = SchemaField(field['name'], helper)
        self.schema_registry[field['name']] = s

    def build_schema(self):
        if not self.schema:
            self._load_schema()
        for field in self.schema['fields']:
            try:
                self._build_and_register_schema_field(field)
            except:
                pass

    def _parse_row_dict(self, row_dict, fieldnames):
        parsed = {}
        for key, value in row_dict.items():
            if key in fieldnames:
                try:
                    parsed[key] = self.schema_registry[key].helper(value)
                except ValueError as e:
                    self.logged[self.rows_processed] = {'field': key, 'value': value, 'error': e, 'row': row_dict}
            else:
                pass
        # parsed['InterventionTime'] = parsed['InterventionDateTime'].strftime("%H:%M")
        return parsed

    # TODO ADD IN FLAG PARSING FOR HANDLING THE WRITING THE COPY FILE. FILTER ON 'DROP'
    def _clean_file(self, outfile, forload, crosswalk):
        if not self.schema:
            self.build_schema()
        file_path = self.config['raw_dir'] + '/' + self.schema['file_name']
        self.rows_processed = 0
        if outfile:
            outfile = "{}/{}".format(self.config['clean_dir'], outfile)
        else:
            outfile = "{}/ctrp3_cleaned_{}.csv".format(self.config['clean_dir'], self.config['version'])
        if forload:
            fieldnames = [x['name'] for x in self.schema['fields'] if not x['db_field'] == 'drop']
        else:
            fieldnames = [x['name'] for x in self.schema['fields']]
        with open(outfile, 'w') as cleanfile:
            cw = csv.DictWriter(cleanfile, fieldnames=fieldnames)
            cw.writeheader()
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        parsed_row = self._parse_row_dict(row, set(fieldnames))
                        cw.writerow(parsed_row)
                    except ValueError as e:
                        pass
                    self.rows_processed += 1

    def field_mapping(self, outfile=None):
        if not self.schema:
            self.build_schema()
        mapping = {v['db_field']: v['name'] for v in self.schema['fields'] if not v['db_field'] == 'drop'}
        if outfile:
            with open(outfile, 'w') as todump:
                json.dump(mapping, todump)
        else:
            print(mapping)

    def clean(self, outfile=None, forload=False, crosswalk=False):
        self._clean_file(outfile, forload, crosswalk)


def test():
    c = Cleaner("/Users/scuerda/Work/Projects/ctrp3_data_prep/config.yml")
    c.build_schema()
    c.clean('loading_v2.csv', True)
    return c
