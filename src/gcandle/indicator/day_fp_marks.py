from gcandle.objects.Indicator_service_objects import DAY_FP_MARK_SERVICES
from gcandle.objects.data_service_objects import SECURITY_DATA_READ_SERVICE


def update_all_foot_peak_marks():
    codes = SECURITY_DATA_READ_SERVICE.read_security_codes()
    for fp_mark_service in DAY_FP_MARK_SERVICES:
        fp_mark_service.update_all_codes(codes)


def recreate_all_foot_peak_marks():
    codes = SECURITY_DATA_READ_SERVICE.read_security_codes()
    for fp_mark_service in DAY_FP_MARK_SERVICES:
        fp_mark_service.recreate_all_codes(codes)


if __name__=="__main__":
    recreate_all_foot_peak_marks()
    # update_all_foot_peak_marks()
    # update_foot_peak_marks('300811', 20, '2019-01-01', '2020-07-30')
    pass
