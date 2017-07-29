class SearchFunctions(object) :
    def __init__(self, query):
        self.query = query

    def isMatch(self, s):
        return self.query in s

    def getMatchesFunctionReference(self, rdd):
        # don't do this, spark references all of self
        return rdd.filter(self.isMatch)

    def getMatchesFieldReference(self, rdd):
        # don't do this, spark references all of self
        return rdd.filter(lambda x: self.query in x)

    def getMatchesNoReference(self, rdd):
        # do this
        query = self.query
        return rdd.filter(lambda x: query in x)
