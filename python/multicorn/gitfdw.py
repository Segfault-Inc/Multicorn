from . import ForeignDataWrapper
import brigit


class GitFdw(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(GitFdw, self).__init__(fdw_options, fdw_columns)
        self.path = fdw_options["path"]
        self.encoding = fdw_options.get("encoding", "utf-8")

    def execute(self, quals):
        def enc(unicode_str):
            return unicode_str.encode(self.encoding)
        for log in  brigit.Git(self.path).pretty_log():
            yield {
                'author_name': enc(log["author"]['name']),
                'author_email': enc(log["author"]['email']),
                'message': enc(log['message']),
                'hash': enc(log['hash']),
                'date': log['datetime'].isoformat()
            }
