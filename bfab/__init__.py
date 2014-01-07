#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

"""
Commands for:

- building and publishing virtual environments
- sync'ing application code
- managing (e.g. start, stop, etc.) the api service
- managing (e.g. start, stop, etc.) the workers

on hosts.

Here's an example of updating all running hosts attached to a load-balancer:

.. code:: bash

    fab hosts:lb=bapi-test-i code_sync

Here's one where just echo which hosts haven been selected by our criteria:

.. code:: bash

    fab hosts:env=test who

Here's one where we say exactly which host(s) to target and disable their
service(s):

.. code:: bash

    fab -H i-1a5c0f47.vandelay.io,bapi-test-10-3-104-23.vandelay.io,10.3.104
    .56 svc_disable

"""
__version__ = '0.1.0'

import os
import time

import boto.ec2.elb
from fabric import api


# statics

class Context(dict):

    def __getattr__(self, item):
        return self[item]


ctx = Context(

    app_name=None,

    STARTUP_DELAY_SECS=5,

    WAIT_DEFAULT_TIMEOUT_SECS=60,

    WAIT_POLL_FREQ_SECS=5,

    HEALTH_FILE='/var/lib/app/health',

    DOMAIN='example.com',

    WORKERS=[],

    AWS_ENVIRONMENT_TAG='ChefEnvironment',

    AWS_DISABLED_TAG='Disabled',

    AWS_API_SUBNET_IDS=[],

    AWS_WORKER_SUBNET_IDS=[],

    AWS_VPC_ID='vpc-asd',

    AWS_GROUP_ID=None,

    AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID', None),

    AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY', None),

    S3_BUCKET='company.debs',

    S3_ENDPOINT='s3-us-west-1.amazonaws.com',
)

# environment

api.env.user = 'deploy'

api.env.region_name = 'us-west-1'

api.env.instances = None

api.env.lbs = None


# common tasks

@api.task
def hosts(env=None, lb=None, subenets=None):
    """
    Selects hosts to target.

    :param env: The environment from which hosts should be *included*. All by
                default. Should be one of 'prod', 'test', 'stage', 'dev'.
    :param lb: The load-balancer whose attached hosts should be *included*.
    """
    populate_lbs()
    if lb:
        lb = resolve_lb(lb)
    tags = {}
    if env:
        tags['tag:' + ctx.AWS_ENVIRONMENT_TAG] = env
    populate_instances(tags=tags, lb=lb, subenets=subenets)
    # HACK: dns resolution does not seem to be working for all instances
    #api.env.hosts = [i.id + '.' + ctx.DOMAIN for i in api.env.instances]
    api.env.hosts = [
        inst.interfaces[0].private_ip_address for inst in api.env.instances
    ]
    for instance in api.env.instances:
        print instance, instance.tags


@api.task
def who():
    """
    Echos hosts that will be targeted by commands.
    """
    pass


@api.task
def code_sync(branch='release', commit='HEAD', clear_cached='t'):
    clear_cached = parse_flag(clear_cached)
    with api.cd('~/' + ctx.app_name):
        api.run('git fetch')
        api.run('git checkout ' + branch)
        if hash != 'HEAD':
            result = api.run(
                'git branch --contains {} | grep {} | wc -l'.format(
                    commit, branch,
                )
            )
            if int(result.strip()) == 0:
                raise ValueError(
                    'Commit "{}" is not a part of "{}" branch!'.format(
                        commit, branch
                    )
                )
            api.run('git checkout ' + commit)
        if clear_cached:
            with api.settings(shell='bash -i -c'):
                api.run("find -type f -regex '.+\.pyc' -exec rm -rf {} \;")


@api.task
def code_stat():
    with api.cd('~/{name}'.format(name=ctx.app_name)):
        api.run('echo `git rev-parse --abbrev-ref HEAD`:`git rev-parse '
                '--verify HEAD`')


@api.task
@api.parallel
def shells():
    """
    Ghetto detects whether any shell(s) are running.
    """
    with api.settings(shell='bash -i -c'):
        api.run('[ -z `pgrep -f "^python.*shell$" -u deploy` ]')


@api.task
def migrate_db():
    with api.cd('~/' + ctx.app_name):
        with api.settings(shell='bash -i -c'):
            api.run('./scripts/migrate-db upgrade')


# service tasks

@api.task
def svc_hosts(env=None, lb=None):
    hosts(env=env, lb=lb, subenets=ctx.AWS_API_SUBNET_IDS)


@api.task
def svc_start(skip_enable='f', wait='t'):
    """
    Starts the service.

    :param skip_enable: Flag indicating whether to skip enabling the host.
    :param wait: Flag indicating whether to wait for host to roll into its lbs.
    """
    api.run('service {} start; sleep {}'.format(
        ctx.app_name, ctx.STARTUP_DELAY_SECS
    ))
    api.run('service {} start'.format(ctx.app_name))
    skip_enable = parse_flag(skip_enable)
    if not skip_enable:
        svc_enable()
        wait_in_lbs(parse_wait(wait))


@api.task
def svc_stop(skip_disable='f', wait='t'):
    """
    Stops the service.

    :param skip_disable: Flag indicating whether to skip disabling the host.
    :param wait: Flag indicating whether to wait for host to fall out of its
                 load-balancers.
    """
    skip_disable = parse_flag(skip_disable)
    if not skip_disable:
        svc_disable()
        wait_out_lbs(parse_wait(wait))


@api.task
def svc_reload():
    """
    Reloads the service.
    """
    api.run('service {} reload'.format(ctx.app_name))


@api.task
def svc_restart():
    """
    Hard restarts the service.
    """
    svc_disable()
    api.run('service {} restart; sleep {}'.format(
        ctx.app_name, ctx.STARTUP_DELAY_SECS
    ))
    svc_enable()


@api.task
def svc_up(branch='release', commit='HEAD', restart='f'):
    """
    Checks out code and reload or restarts the service.

    :param branch: Branch to checkout. Defaults to "release".
    :param commit: Commit hash within the branch to sync to, defaults to "HEAD".
    :param restart: Flag indicating whether the service should be restarted or
                    just reloaded (the default).
    """
    restart = parse_flag(restart)
    code_sync(branch, commit)
    # TODO: enable this
    #migrate_db()
    if restart:
        svc_restart()
    else:
        svc_reload()
    svc_stat()


@api.task
def svc_stat():
    """
    Prints service status.
    """
    code_stat()
    api.run('service {} status'.format(ctx.app_name))
    api.run('curl 127.0.01:5000/health')


@api.task
def svc_enable(wait='t'):
    """
    Enabled service for traffic.

    :param wait: Flag indicating whether to wait for host to roll into its
                 load-balancers.
    """
    api.run('echo -n "finding your center" > {0}'.format(ctx.HEALTH_FILE))
    wait_in_lbs(parse_wait(wait))


@api.task
def svc_disable(wait='t'):
    """
    Disables service from serving traffic.

    :param wait: Flag indicating whether to wait for host to fall out of its
                 load-balancers.
    """
    wait = parse_wait(wait)
    api.run('[ ! -f {0} ] || rm {0}'.format(ctx.HEALTH_FILE))
    wait_out_lbs(wait)


# worker helpers


@api.task
def wrk_hosts(env=None, lb=None):
    hosts(env=env, lb=lb, subenets=ctx.AWS_WORKER_SUBNET_IDS)


@api.task
def wrk_up(branch='release', commit='HEAD'):
    """
    Checks out code and restarts all workers.

    :param branch: Branch to checkout. Defaults to "release".
    :param commit: Commit hash within the branch to sync to, defaults to "HEAD".
    """
    code_sync(branch, commit)
    wrk_restart()
    wrk_stat()


@api.task
def wrk_stat(*workers):
    """
    Prints status about the requested workers, or all if none are specified.
    """
    code_stat()
    for name in workers or ctx.WORKERS:
        api.run('supervisorctl status {}; sleep 1'.format(name))


@api.task
def wrk_start(*workers):
    """
    Starts the requested workers, or all if none are specified.
    """
    for name in workers or ctx.WORKERS:
        api.run('supervisorctl start {}; sleep 1'.format(name))


@api.task
def wrk_stop(*workers):
    """
    Stops the requested workers, or all if none are specified.
    """
    for name in workers or ctx.WORKERS:
        api.run('supervisorctl stop {}; sleep 1'.format(name))


@api.task
def wrk_restart(*workers):
    """
    Restarts the requested workers, or all if none are specified.
    """
    for name in workers or ctx.WORKERS:
        api.run('supervisorctl stop {}; sleep 1'.format(name))


# package tasks


@api.task
def pkg_build(version, branch='release', commit='HEAD', publish=False):
    """
    Builds and downloads a deb of app_name (w/o the virtualenv).

    :param version: Release version (e.g. 1.0.0).
    :param branch: git branch from which to package. Defaults to 'release'.
    :param commit: git commit commit from which to package. Defaults to 'HEAD'.
    """
    code_sync(branch=branch, commit=commit)
    if commit == 'HEAD':
        with api.cd('~/' + ctx.app_name):
            commit = api.run('git rev-parse HEAD')
    with api.cd('~'):
        api.run(
            '[ ! -f {app_name}_1.{version}_all.deb ] || '
            'rm -f {app_name}_1.{version}_all.deb'
                .format(app_name=ctx.app_name, version=version)
        )
        rv = api.run(
            'fpm -s dir -t deb -n {package_name} -v {version} '
            '-a all -x "*.git" -x "*.pyc"  '
            '--description "{app_name} @ {branch}:{commit}" '
            '--deb-user={user} '
            '--deb-group={user} '
            '~/{package_name}'
                .format(
                app_name=ctx.app_name,
                package_name=ctx.app_name,
                version=version,
                user=api.env.user,
                branch=branch,
                commit=commit,
            )
        )
    file_name = rv.split('"')[-2]
    if publish:
        pkg_publish(file_name)


@api.task
def pkg_build_venv(version, branch='release', commit='HEAD', publish=False):
    """
    Builds and downloads a deb of app_name virtualenv (w/o the lib).

    :param version: Release version (e.g. 1.0.0).
    :param branch: git branch from which to package. Defaults to 'release'.
    :param commit: git commit commit from which to package. Defaults to 'HEAD'.
    """
    code_sync(commit=commit, branch=branch)
    if commit == 'HEAD':
        with api.cd('~/' + ctx.app_name):
            commit = api.run('git rev-parse HEAD')
    with api.cd('~'):
        api.run(
            '[ ! -f {app_name}-venv_{version}_amd64.deb ] || '
            'rm -f {app_name}-venv_{version}_amd64.deb'
                .format(app_name=ctx.app_name, version=version)
        )
    rv = api.run(
        'fpm -s python -t deb -n {app} -v {version} '
        '--description "{app_name} virtual environment @ {branch}:{commit}" '
        '--deb-user={user} '
        '--deb-group={user} '
        '-s dir ~/.virtualenvs/{venv} '
            .format(
            app_name=ctx.app_name,
            app=ctx.app_name + '_venv',
            venv=ctx.app_name,
            version=version,
            user=api.env.user,
            branch=branch,
            commit=commit,
        )
    )
    file_name = rv.split('"')[-2]
    if publish:
        pkg_publish(file_name)


@api.task
def pkg_publish(file_name):
    """
    Uploads a deb package to the s3 bucket backing our apt repo. Note that:

        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY

    must both be  set in your environment *and* have write permissions to the
    s3 bucket.

    :param file_name: Name of built deb file to publish.
    """
    if ctx.AWS_ACCESS_KEY_ID is None:
        raise Exception('Your environment is missing AWS_ACCESS_KEY_ID')
    if ctx.AWS_SECRET_ACCESS_KEY is None:
        raise Exception('Your environment is missing AWS_SECRET_ACCESS_KEY')
    with api.cd('~'):
        api.run(
            'deb-s3 publish {file_name} '
            '--bucket={s3_bucket} '
            '--access-key-id={s3_access_key} '
            '--secret-access-key={s3_secret_key} '
            '--endpoint={s3_endpoint} '
            '--visibility=private '
            '--arch={arch}'
                .format(
                file_name=file_name,
                s3_bucket=ctx.S3_BUCKET,
                s3_access_key=ctx.AWS_ACCESS_KEY_ID,
                s3_secret_key=ctx.AWS_SECRET_ACCESS_KEY,
                s3_endpoint=ctx.S3_ENDPOINT,
                arch='amd64',
            )
        )


# generic helpers

def parse_flag(flag):
    if flag.lower() in (True, 1, '1', 't', 'true'):
        return True
    if flag.lower() in (False, 0, '0', 'f', 'false'):
        return False
    raise ValueError('Invalid flag value "{}"'.format(flag))


def parse_wait(raw):
    try:
        return int(raw)
    except (ValueError, TypeError):
        flag = parse_flag(raw)
        if flag:
            return ctx.WAIT_DEFAULT_TIMEOUT_SECS
        return 0


# aws helpers

def populate_instances(
        tags=None,
        lb=None,
        exclude_disabled=True,
        subenets=None,
):
    def local_filter(instance):
        if subenets and instance.subnet_id not in subenets:
            return False
        if instance.tags.get(ctx.AWS_DISABLED_TAG, None) is not None:
            return False
        if lb:
            return any(instance.id == i.id for i in lb.instances)
        return True
    if api.env.instances:
        return api.env.instances
    remote_filter = {
        'vpc-id': ctx.AWS_VPC_ID,
        'instance-state-name': 'running',
    }
    if ctx.AWS_GROUP_ID:
        remote_filter['instance.group-id'] = ctx.AWS_GROUP_ID
    if tags:
        remote_filter.update(tags)
    cxn = boto.ec2.connect_to_region(api.env.region_name)
    instances = [
        instance.instances[0]
        for instance in cxn.get_all_instances(filters=remote_filter)
        if local_filter(instance.instances[0])
    ]
    api.env.instances = instances
    return api.env.instances


def populate_lbs():
    if api.env.lbs is not None:
        return api.env.lbs
    cxn = boto.ec2.elb.connect_to_region(api.env.region_name)
    api.env.lbs = [
        lb for lb in cxn.get_all_load_balancers()
        if lb.instances is not None
    ]
    return api.env.lbs


def resolve_lb(hint):
    return resolve_lbs(hint)[0]


def resolve_lbs(*hints):
    mapping = dict((lb.name, lb) for lb in api.env.lbs)
    lbs = []
    for hint in hints:
        if hint in mapping:
            lbs.append(mapping[hint])
            continue
        raise ValueError('Unknown load balancer "{}"'.format(hint))
    return lbs


def instance_lbs(instance):
    return [
        lb for lb in api.env.lbs
        if any(instance.id == i.id for i in lb.instances)
    ]


def current_instance():
    populate_instances()
    populate_lbs()
    host_string = api.env.host_string
    for i in api.env.instances:
        if 'Name' in i.tags and i.tags['Name'].startswith(host_string):
            break
        if i.private_ip_address.startswith(host_string):
            break
        if i.private_ip_address.replace('.', '-') in host_string:
            break
    else:
        i = None
    return i


def wait_in_lbs(timeout):
    def in_service(states):
        return (
            not states and
            states[0].state == 'InService'
        )

    wait_xx_lbs(timeout, in_service)


def wait_out_lbs(timeout):
    def out_of_service(states):
        return (
            not states or
            states[0].state == 'OutOfService'
        )

    wait_xx_lbs(timeout, out_of_service)


def wait_xx_lbs(timeout, health):
    instance = current_instance()
    if instance is None:
        return
    lbs = instance_lbs(instance)
    et = time.time() + timeout
    while True:
        lbs = [
            lb for lb in lbs
            if not health(lb.get_instance_health([instance.id]))
        ]
        if not lbs:
            break
        if time.time() > et:
            raise Exception(
                'Timed out after {} sec(s) waiting on host "{}" '
                'health for lb(s) {}'.format(
                    timeout,
                    api.env.host_string,
                    ', '.join((lb.name for lb in lbs))
                )
            )
        print '[%s] local: waiting %s sec(s) for lb(s) %s' % (
            api.env.host_string, ctx.WAIT_POLL_FREQ_SECS, ', '.join(
                (lb.name for lb in lbs)
            ),
        )
        time.sleep(ctx.WAIT_POLL_FREQ_SECS)
