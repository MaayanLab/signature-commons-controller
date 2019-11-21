import urllib.parse as urllib_parse

class ParsedUrl:
  ''' A convenient object for manipulating urls -- works
  like urlparse + parse_qs but mutable

  Example:
    url = ParsedUrl('http://google.com/?q=hello+world&a=this#anchor')
    del url.fragment
    del url.query['a']
    url.query['q'] = 'hello'
    url.scheme = 'https'
    url.username = 'hello'
    url.password = 'world'
    print(url) => https://hello:world@google.com/?q=hello
  '''
  def __init__(self, url):
    self._parsed = urllib_parse.urlparse(url)
    self._scheme = self._parsed.scheme
    self._netloc = self._parsed.netloc
    self._path = self._parsed.path
    self._query = urllib_parse.parse_qs(self._parsed.query)
    self._fragment = self._parsed.fragment
  #
  def __delattr__(self, attr):
    if attr in ['username', 'password', 'port', 'path', 'query', 'fragment']:
      setattr(self, attr, None)
    else:
      super().__delattr__(attr)
  #
  @property
  def scheme(self):
    return self._scheme
  #
  @scheme.setter
  def scheme(self, scheme):
    self._scheme = scheme
  #
  @property
  def netloc(self):
    return self._netloc
  #
  @netloc.setter
  def netloc(self, netloc):
    self._netloc = netloc
    self._parsed = urllib_parse.urlparse(str(self))
  #
  def _update_netloc(self, **changes):
    parsed = dict(
      {
        'username': self._parsed.username,
        'password': self._parsed.password,
        'hostname': self._parsed.hostname,
        'port': self._parsed.port,
      },
      **changes,
    )
    netloc = ''
    if parsed['username']:
      netloc = parsed['username']
      if parsed['password']:
        netloc += ':' + parsed['password']
      netloc += '@'
    netloc += parsed['hostname']
    if parsed['port']:
      netloc += ':' + str(parsed['port'])
    self.netloc = netloc
  #
  @property
  def hostname(self):
    return self._parsed.hostname
  #
  @hostname.setter
  def hostname(self, hostname):
    self._update_netloc(hostname=hostname)
  #
  @property
  def port(self):
    return self._parsed.port
  #
  @port.setter
  def port(self, port):
    self._update_netloc(port=port)
  #
  @property
  def username(self):
    return self._parsed.username
  #
  @username.setter
  def username(self, username):
    self._update_netloc(username=username)
  #
  @property
  def password(self):
    return self._parsed.password
  #
  @password.setter
  def password(self, password):
    self._update_netloc(password=password)
  #
  @property
  def path(self):
    return self._path
  #
  @path.setter
  def path(self, path):
    self._path = path
  #
  @property
  def query(self):
    return self._query
  #
  @query.setter
  def query(self, query):
    if query:
      self._query = urllib_parse.parse_qs(
        urllib_parse.urlencode(query)
      )
    else:
      self._query = None
  #
  @property
  def fragment(self):
    return self._fragment
  #
  @fragment.setter
  def fragment(self, fragment):
    self._fragment = fragment
  #
  def __str__(self):
    url = self._scheme + '://' + self._netloc
    if self._path:
      url += self._path
    if self._query:
      url += '?' + urllib_parse.urlencode([
        (k, v)
        for k, V in self._query.items()
        for v in (V if type(V) == list else [V])
      ])
    if self._fragment:
      url += '#' + self._fragment
    return url
  #
  def __repr__(self):
    return str(self)


