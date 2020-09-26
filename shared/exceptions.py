# TODO -- co.api.InvalidResponse is used in (client) public code.  This should
# move to private.


class ClientError(Exception):
    def __init__(self, status_code, message):
        Exception.__init__(self)
        self.status_code = status_code
        self.message = message

    def to_dict(self):
        return {"message": self.message}
