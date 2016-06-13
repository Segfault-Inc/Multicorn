"""
A Git foreign data wrapper

"""

from . import ForeignDataWrapper
import brigit


class GitFdw(ForeignDataWrapper):
    """A Git foreign data wrapper.

    The git foreign data wrapper accepts the following options:

    path        --  the absolute path to the git repo. It must be readable by
                    the user running postgresql (usually, postgres).
    encoding    --  the file encoding. Defaults to "utf-8".

    """

    def __init__(self, fdw_options, fdw_columns):
        super(GitFdw, self).__init__(fdw_options, fdw_columns)
        self.path = fdw_options["path"]
        self.encoding = fdw_options.get("encoding", "utf-8")

    def execute(self, quals, columns):
        def enc(unicode_str):
            """Encode the string in the self given encoding."""
            return unicode_str.encode(self.encoding)
        for log in brigit.Git(self.path).pretty_log():
            yield {
                'author_name': enc(log["author"]['name']),
                'author_email': enc(log["author"]['email']),
                'message': enc(log['message']),
                'hash': enc(log['hash']),
                'date': log['datetime'].isoformat()
            }
