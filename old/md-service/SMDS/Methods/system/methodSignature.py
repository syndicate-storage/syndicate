# modifications by Jude Nelson

from SMDS.parameter import Parameter, Mixed
from SMDS.method import Method, xmlrpc_type

class methodSignature(Method):
    """
    Returns an array of known signatures (an array of arrays) for the
    method name passed. If no signatures are known, returns a
    none-array (test for type != array to detect missing signature).
    """

    roles = []
    accepts = [Parameter(str, "Method name")]
    returns = [Parameter([str], "Method signature")]

    def __init__(self, api):
        Method.__init__(self, api)
        self.name = "system.methodSignature"

    def possible_signatures(self, signature, arg):
        """
        Return a list of the possible new signatures given a current
        signature and the next argument.
        """

        if isinstance(arg, Mixed):
            arg_types = [xmlrpc_type(mixed_arg) for mixed_arg in arg]
        else:
            arg_types = [xmlrpc_type(arg)]

        return [signature + [arg_type] for arg_type in arg_types]

    def signatures(self, returns, args):
        """
        Returns a list of possible signatures given a return value and
        a set of arguments.
        """

        signatures = [[xmlrpc_type(returns)]]

        for arg in args:
            # Create lists of possible new signatures for each current
            # signature. Reduce the list of lists back down to a
            # single list.
            signatures = reduce(lambda a, b: a + b,
                                [self.possible_signatures(signature, arg) \
                                 for signature in signatures])

        return signatures

    def call(self, method):
        function = self.api.callable(method)
        (min_args, max_args, defaults) = function.args()

        signatures = []

        assert len(max_args) >= len(min_args)
        for num_args in range(len(min_args), len(max_args) + 1):
            signatures += self.signatures(function.returns, function.accepts[:num_args])

        return signatures
