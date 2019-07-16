# Description: dovecot netdata python.d module
# Author: Kyle Agronick (agronick)
# SPDX-License-Identifier: GPL-3.0+

# Gearman Netdata Plugin

from bases.FrameworkServices.SocketService import SocketService
from copy import deepcopy

update_every = 3
priority = 60500


CHARTS = {
    'workers': {
        'options': [None, 'Total Workers', 'workers', 'workers', 'gearman.workers', 'stacked'],
        'lines': [
            ['total_queued', 'Queued', 'absolute'],
            ['total_idle', 'Idle', 'absolute'],
            ['total_active', 'Active', 'absolute'],
        ]
    },
}


def job_chart_template(job_name):
    return {
        'options': [None, job_name, 'workers', 'workers', 'gearman.{0}'.format(job_name), 'stacked'],
        'lines': [
            ['{0}_queued'.format(job_name), 'Queued', 'absolute'],
            ['{0}_idle'.format(job_name), 'Idle', 'absolute'],
            ['{0}_active'.format(job_name), 'Active', 'absolute'],
        ]
    }


class Service(SocketService):
    def __init__(self, configuration=None, name=None):
        super(Service, self).__init__(configuration=configuration, name=name)
        self.request = "status\n"
        self._keep_alive = True

        self.host = self.configuration.get('host', 'localhost')
        self.port = self.configuration.get('port', 4730)

        self.active_jobs = set()
        self.definitions = deepcopy(CHARTS)
        self.order = ['workers']

    def _get_data(self):
        """
        Format data received from socket
        :return: dict
        """

        output = {
            'total_queued': 0,
            'total_idle': 0,
            'total_active': 0,
        }

        for job in self._get_worker_data():
            job_name = job[0]

            has_data = any(job[1:])
            if job_name in self.active_jobs and not has_data:
                self._remove_chart(job_name)

            elif has_data:

                if job_name not in self.active_jobs:
                    self._add_chart(job_name)

                job_data = self._build_job(job)
                output.update(job_data)

                for sum_value in ('queued', 'idle', 'active'):
                    output['total_{0}'.format(sum_value)] += job_data['{0}_{1}'.format(job_name, sum_value)]

        return output

    def _get_worker_data(self):
        """
        Split the data returned from Gearman into a list of lists
        :return: list
        """

        try:
            raw = self._get_raw_data()
        except (ValueError, AttributeError):
            return

        if raw is None:
            self.debug("Gearman returned no data")
            return

        for line in sorted([job.split() for job in raw.splitlines()][:-1], key=lambda x: x[0]):
            line[1:] = map(int, line[1:])
            yield line

    def _build_job(self, job):
        """
        Get the status for each job
        :return: dict
        """

        total, running, available = job[1:]

        idle = available - running
        pending = total - running

        return {
            '{0}_queued'.format(job[0]): pending,
            '{0}_idle'.format(job[0]): idle,
            '{0}_active'.format(job[0]): running,
        }

    def _add_chart(self, job_name):
        job_key = 'job_{0}'.format(job_name)
        template = job_chart_template(job_name)
        new_chart = self.charts.add_chart([job_key] + template['options'])
        for dimension in template['lines']:
            new_chart.add_dimension(dimension)

    def _remove_chart(self, job_name):
        job_key = 'job_{0}'.format(job_name)
        self.charts[job_key].obsolete()


