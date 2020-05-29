import argparse
import json
import subprocess
from time import sleep


_DESC = 'Ensure standby_replay assignments to ranks matches a specification'


def get_fs_map():
    # check if we can get a ceph fs dump
    fs_dump = subprocess.run(['ceph', 'fs', 'dump', '--format', 'json'],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    if fs_dump.returncode != 0:
        print(f'ERROR "ceph fs dump" returned rc: {fs_dump.returncode}')
        print(f'ERROR {fs_dump.args}')
        print(f'ERROR {fs_dump.stderr}')
        exit(1)

    return json.loads(fs_dump.stdout)


def get_fs(name, fsmap):
    for fs in fsmap['filesystems']:
        if fs['mdsmap']['fs_name'] == name:
            print(f'Found requested file system {name}')
            return fs['mdsmap']

    print(f'ERROR could not find file system with name {name}')
    exit(1)


def get_current_standby_assignment(fs):
    return {mds['rank']: mds['name'] for _gid, mds in fs['info'].items() if
            mds['state'] == 'up:standby-replay'}


def get_cold_standby(fs):
    return fs['standbys'][0]['name']


def check(current, wanted, cold, last=[]):
    restart_rank = ''
    if cold in wanted:
        # current cold is wanted as replay
        wanted_rank = wanted[cold]
        restart_rank = current[wanted_rank]
    else:
        # all wanted standby_replays are standby_replays, but assigned to the wrong ranks
        current_standbys = current.values()
        for mds, rank in wanted.items():
            if mds not in current_standbys:
                print((f'mds {mds} is not listed in current standbys, its '
                       'either active or doesn\'t exist. Skipping...'))
                continue
            if current[rank] == mds:
                # this one is where we want it
                print(f'mds {mds} is assigned to the wanted rank {rank}')
                continue
            if mds in last and mds == last[0]:
                print(f'mds {mds} was just restarted, trying to find another one')
                continue

            print(f'chose mds {mds} for restart')
            restart_rank = mds
            break
    if not restart_rank:
        return last

    print(f'{restart_rank} is not the wanted standby_replay, will restart')
    subprocess.run(['ceph', 'mds', 'fail', restart_rank],
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)

    return last + [restart_rank]


class MDSStandbyFor(object):

    fs = ''
    args = argparse.Namespace()

    def __init__(self):
        parser = argparse.ArgumentParser(description=_DESC)

        parser.add_argument(
            'fs',
            help='file system name')
        parser.add_argument(
            'standby_assignment',
            type=json.loads,
            help='A simple json dictionary with <mds_name>: <rank> pairs')

        self.args = parser.parse_args()

        self.main()

    def main(self):
        last = []
        for _ in range(10):
            # stop after 10 attempts
            fsmap = get_fs_map()

            if not fsmap['standbys']:
                print('ERROR no unassigned standbys found in fsmap')
                exit(1)

            if len(fsmap['standbys']) != 1:
                print(f'ERROR no can only work with 1 unassigned standby, got {len(fsmap["standbys"])}')
                exit(1)

            fs = get_fs(self.args.fs, fsmap)

            current = get_current_standby_assignment(fs)
            cold = get_cold_standby(fsmap)
            new_last = check(current, self.args.standby_assignment, cold)
            if last == new_last:
                print('done')
                exit(0)
            elif not new_last:
                print('nothing was restarted, assuming we\'re done')
                exit(0)
            else:
                last = new_last
                sleep(5)
        exit(1)


if __name__ == "__main__":
    MDSStandbyFor()
