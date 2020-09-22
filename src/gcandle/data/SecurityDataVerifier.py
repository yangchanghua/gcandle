from gcandle.objects.basic_objects import DB_CLIENT
from gcandle.utils.multi_process_pool import execute_tasks

BASIC_DATA_COLUMNS = ['open', 'close', 'low', 'high', 'amount']

START_DATE = '2001-01-01'


class SecurityDataVerifier:
    def __init__(self):
        pass

    def compare_basic_data_of_repos(self, code, start, end, repo1, repo2, cols):
        df1 = DB_CLIENT.read_data_by_codes_and_date(repo1, [code], start, end)
        if df1 is None:
            print("no data from {} for {}".format(repo1, code))
            return None
        df1 = df1[cols]
        df2 = DB_CLIENT.read_data_by_codes_and_date(repo2, [code], start, end)
        if df2 is None:
            print("no data from {} for {}".format(repo2, code))
            return None
        df2 = df2[cols]
        res = self.do_compare(df1, df2, cols)
        if res is None:
            print("Something is wrong")
            return
        l, r, d = res
        if len(l) > 0:
            print("### {} Left only ###: \n".format(code), l)
        if len(r) > 0:
            print('### {} Right only ###: \n'.format(code), r)
        if len(d) > 0:
            print('### {} Not matched ###: \n'.format(code), d)
        if len(l) == 0 and len(r) == 0 and len(d) == 0:
            print("{} All matched".format(code))

    def do_compare(self, df1, df2, cols):
        joined = df1.join(df2, how='outer', rsuffix='_r')
        col = cols[0]
        left_only = joined[joined[col + '_r'].isna()]
        right_only = joined[joined[col].isna()]
        both_have = joined.dropna()
        if len(both_have) < 1:
            print('Warning: no data that exists in both dataframes')
            return left_only, right_only, both_have
        else:
            value_not_match = both_have[col] != both_have[col + '_r']
            for col in cols[1:]:
                value_not_match |= both_have[col] != both_have[col + '_r']
            return left_only, right_only, both_have[value_not_match]


def verify_qfq_data():
    repo1 = 'stock_day_qfq'
    repo2 = 'stock_day_qfq_new_stock_day_with_old_xdxr'
    # codes = ['600158']
    execute_tasks(codes, verifier.compare_basic_data_of_repos, start, end, repo1, repo2, BASIC_DATA_COLUMNS)


def verify_xian_backtest():
    repo1 = 'xian_backtest'
    repo2 = 'xian_stock_day_long_fp'
    # codes = ['600158']
    execute_tasks(codes, verifier.compare_basic_data_of_repos, start, end, repo1, repo2, ['open', 'close'])


def verify_fp_marks_data():
    for fpd in [20, 50, 120]:
        repo1 = 'fp_marks_' + str(fpd)
        repo2 = 'indicator_fp_marks_' + str(fpd) + '_day'
        cols = ['fp_type', 'fp_around_' + str(fpd)]
        execute_tasks(codes, verifier.compare_basic_data_of_repos, start, end, repo1, repo2, cols)

def verify_chan_zs():
    repo1 = 'indicator_chan_current_zs_1min'
    repo2 = 'indicator_chan_current_zs_1min_copy'
    # codes = ['600158']
    execute_tasks(codes, verifier.compare_basic_data_of_repos, start, end, repo1, repo2, ['xd_time', 'zs_time', 'zs_low', 'zs_high'])


def verify_chan_fp():
    repo1 = 'indicator_chan_current_fp_1min'
    repo2 = 'indicator_chan_current_fp_1min_copy'
    # codes = ['600158']
    execute_tasks(codes, verifier.compare_basic_data_of_repos, start, end, repo1, repo2, ['pFpTime', 'pFpType', 'pFpPrice'])

if __name__ == '__main__':
    from gcandle.data.SecurityDataService import SecurityDataService
    import random
    dataService = SecurityDataService()
    verifier = SecurityDataVerifier()
    start = '2019-01-01'
    end = '2020-09-31'
    codes = dataService.read_security_codes()
    codes = random.sample(codes, 300)
    codes = ['000001']
    verify_chan_fp()
    # verify_fp_marks_data()

