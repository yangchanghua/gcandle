from gcandle.indicator.fuquan import stock_to_qfq
from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE
from gcandle.utils.multi_process_pool import execute_tasks
import gcandle.utils.date_time_utils as date_time_utils


DAY_QFQ_REPO = 'stock_day_qfq'
MIN_START_DATE = '2006-01-01'


def copy_stock_day_directly(code, start, end):
    stock_data = SECURITY_DATA_READ_SERVICE.read_security_data_for_single(code, start, end)
    DB_CLIENT.update_data_append_newer(stock_data, DAY_QFQ_REPO)


def calc_qfq_and_update(code, start, end):
    stock_data = SECURITY_DATA_READ_SERVICE.read_security_data_for_single(code, start, end)
    if stock_data is not None and len(stock_data) > 0:
        qfq_data = stock_to_qfq(stock_data)
        DB_CLIENT.update_data_append_newer(qfq_data, DAY_QFQ_REPO)


def re_calc_qfq_and_overwrite(code, start, end):
    start = MIN_START_DATE
    end = date_time_utils.Date().as_str()
    stock_data = SECURITY_DATA_READ_SERVICE.read_security_data_for_single(code, start, end)
    qfq_data = stock_to_qfq(stock_data)
    DB_CLIENT.replace_data_by_codes(qfq_data, DAY_QFQ_REPO)


def update_qfq_for_single_code(code, start, end):
    max_qfq_date = DB_CLIENT.read_max_date_for(code, DAY_QFQ_REPO)
    if max_qfq_date is None:
        calc_qfq_and_update(code, start, end)
    else:
        fq_info = SECURITY_DATA_READ_SERVICE.read_fq_info(code, start, end)
        if fq_info is None:
            copy_stock_day_directly(code, start, end)
        else:
            max_fq_info_date = fq_info.index.levels[0].max()
            if str(max_fq_info_date)[0:10] < max_qfq_date:
                copy_stock_day_directly(code, start, end)
            else:
                print("Recreate needed for {}, max qfq date={}, max fq info date={}".format(code, max_qfq_date, max_fq_info_date))
                re_calc_qfq_and_overwrite(code, start, end)


def update_stock_qfq_data():
    end = date_time_utils.Date()
    start = end.get_before(days=15)
    code_list = SECURITY_DATA_READ_SERVICE.read_security_codes()
    execute_tasks(code_list, update_qfq_for_single_code, start.as_str(), end.as_str())


def recreate_stock_qfq_data():
    start = MIN_START_DATE
    end = date_time_utils.Date().as_str()
    init_qfq_repo()
    print('START recreate QFQ data')
    code_list = SECURITY_DATA_READ_SERVICE.read_security_codes()
    execute_tasks(code_list, update_qfq_for_single_code, start, end)
    print('DONE recreate QFQ data')


def init_qfq_repo():
    DB_CLIENT.create_index_for_collection_with_date_and_code(DAY_QFQ_REPO)


if __name__=="__main__":
    # recreate_stock_qfq_data()
    update_stock_qfq_data()
    # update_qfq_for_single_code('300831', '2006-01-01', date_time_utils.Date().as_str())


