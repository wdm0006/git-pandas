from gitpandas import Repository

__author__ = 'willmcginnis'


if __name__ == '__main__':
    repo = Repository(working_dir='git://github.com/apache/flink.git', verbose=True)
    shared_blame = repo.cumulative_blame(extensions=['scala'], num_datapoints=10)
    print(shared_blame.columns.values)
