# coding: utf8
# option_parser follow the design in ysid

__author__ = ['Changsheng Jiang<jiangzuoyan@gmail.com>']

"""
A very general/limited option parser. For any option, we require a
match, a process, and a document. The parser hold the index of last
parsed argument, and call all options' match functions, and sort the
match result, the highest one win. the winner's process function is
called, and it should return the next index, i.e. the index of last
not consumed argument.

Match is any object who can be called with (parser, index, argv), it
should return MATCH_EXACT, MATCH_PREFIX, MATCH_POSITION, or
MATCH_NONE. Match.name should return the name part of option
document.

Process is any object who can be called with (parser, index, argv),
it should return new index, or negative to indicating errors.
Process.meta should return the meta part of option document.

Document is any object with name, meta, and doc properties as string.

Construction of Match from string is provided. The option is joined by
'|', the name is the options with '--' added at prev.

Construction of Document from string is provided. The name and meta is
splited by '==', and the name[==meta] and doc is splited by
'::'. Construction Document from dict is also provided.

Example:

import option_parser
cmd_parser = option_parser.OptionParser(description="a test of option_parser")
cmd_parser.add_help()
value = 20
cmd_parser.add_option("value", scope=locals(), convert=int)
# --value 10 will set value=10

cmd_parser.add_option("VALUE", scope=locals(), dest="value",
                      convert=lambda x:int(x)*10)
# --VALUE 10 will set value 100

# Don't like C++ version, python's lambda and closure are too limited ...
L = locals()
def local_set(n):
   def func(v):
     L[n] = v
   return func
cmd_parser.add_option("value",  func=lambda x:local_set("value"),
                      doc="INT::set value to that value")
"""

__all__ = ["OptionParser", "MATCH_EXACT", "MATCH_PREFIX", "MATCH_POSITION", "MATCH_NONE",
           "to_date", "to_datetime"]

import sys
import traceback
import functools
import inspect
import shlex
import os
import datetime, time

MATCH_EXACT = 1000
MATCH_PREFIX = 100
MATCH_POSITION = 10
MATCH_NONE = 0

if sys.version.startswith("3.1"):
    import collections
    def callable(f):
        return isinstance(f, collections.Callable)

def _writeln(fp, *args):
    fp.write(" ".join(str(i) for i in args) + os.linesep)

def _writeeln(*args):
    sys.stderr.write(" ".join(str(i) for i in args) + os.linesep)

class AsAttribute(object):

    def __init__(self, obj):
        self.__obj = obj

    def __getattr__(self, attr):
        if hasattr(self.__obj, attr):
            return getattr(self.__obj, attr)
        try:
            return self.__obj[attr]
        except:
            raise AttributeError

    def __call__(self, *args, **kargs):
        if callable(self.__obj):
            return self.__obj(*args, **kargs)

    def __repr__(self):
        return "AsAttribute(%s)" % (self.__obj)

def as_attribute(obj):
    if isinstance(obj, AsAttribute):
        return obj
    if callable(obj):
        return obj
    return AsAttribute(obj)

def call_process(process, *args):
    if callable(process):
        return process(*args)
    if hasattr(process, "process"):
        return process.process(*args)
    raise RuntimeError("don't know how to call process type=%s" % type(process))

def call_match(m, *args):
    if callable(m):
        return m(*args)
    if hasattr(m, "match"):
        return m.match(*args)
    return MATCH_NONE

class StringMatch(object):

    def __init__(self, s):
        self.s = s
        self.name = "--" + s

    def __call__(self, parser, idx, argv):
        """return match priority"""
        opts = self.s.split("|")
        ret = MATCH_NONE
        opt = argv[idx].lstrip("-")
        for o in opts:
            if o == opt:
                return MATCH_EXACT
            if o.startswith(opt):
                ret = MATCH_PREFIX
        return ret

    def __repr__(self):
        return "StringMatch(%s)" % self.s

class Option(object):

    def __init__(self, **kargs):
        match = kargs.get("match")
        doc = kargs.get("doc", "")
        process = kargs.get("process")
        group = kargs.get("group", "")

        if isinstance(match, str):
            match = StringMatch(match)
        if not isinstance(match, bool):
            if not hasattr(match, "name"):
                match = as_attribute(match)
        if process:
            if not hasattr(process, "meta"):
                process = as_attribute(process)
        if isinstance(doc, str):
            doc = StringDoc(doc)
        if not isinstance(doc, bool):
            if not hasattr(doc, "name"):
                doc = as_attribute(doc)
            if not doc.name and hasattr(match, "name"):
                doc.name = match.name
            if not doc.meta and hasattr(process, "meta"):
                doc.meta = process.meta
            if doc.meta == "!": doc.meta = ""
        self.match = match
        self.doc = doc
        self.process = process
        self.group = group

    def __call__(self, parser, idx, argv):
        """
        return new-index or negative, or throw
        """
        r = call_process(self.process, parser, idx, argv)
        assert isinstance(r, int)
        return r

    def __repr__(self):
        return "Option(%s,%s,%s, %s)" % (
            self.match, self.process, self.doc, self.group)

class FuncMatch(object):

    def __init__(self, func, name=None):
        self.func = func
        self.name = name

    def __call__(self, parser, idx, argv):
        """return match priority"""
        r = self.func(argv[idx])
        if isinstance(r, int):
            return r
        if r:
            return MATCH_EXACT
        return  MATCH_NONE

class FuncProcess(object):

    def __init__(self, func, **kargs):
        self.convert = kargs.get("convert", lambda x: x)
        self.func = func
        self.nary = kargs.get("nary") or len(inspect.getargspec(func).args)
        self.with_opt = kargs.get("with_opt", 0)
        self.var = kargs.get("var", 0)
        self.var_convert = kargs.get("var_convert", None)

    def __call__(self, parser, idx, argv):
        nary = self.nary
        if self.var:
            # in var mode, all args not startswith '-' are gathered and passed as a list
            assert nary >= 1
            nary -= 1
        shift = 1 - self.with_opt
        if idx + nary + shift > len(argv):
            raise RuntimeError("option at %s('%s') func requires %s args, but only %s ..." % (
                idx, argv[idx], nary, len(argv) - idx - shift))
            return -1
        func_argv = argv[idx + shift: idx + shift + nary]
        if self.var:
            v = []
            while idx + nary + shift < len(argv):
                opt = argv[idx + nary + shift]
                if opt.startswith("-"):
                    break
                v.append(opt)
                idx += 1
            if self.var_convert:
                v = [self.var_convert(i) for i in v]
            func_argv.append(v)
        if not shift and nary:
            func_argv[0].lstrip("-")
        func_argv = self.convert(func_argv)
        r = self.func(*func_argv)
        if r is None:
            r = 1
        if isinstance(r, int):
            if r <= 0: return -1
        if not r: return -1
        return idx + nary + shift

    def get_meta(self):
        s = ""
        if not self.with_opt:
            try:
                s = " ".join(i.upper().lstrip("_") for i in inspect.getargspec(self.func).args)
            except:
                pass
        return s

    meta = property(get_meta)

class StringDoc(object):

    def __init__(self, s):
        self.doc = s
        self.name = ""
        self.meta = ""
        if "::" in self.doc.split("\n", 1)[0]:
            self.name = self.doc.split("\n", 1)[0].split("::", 1)[0]
            self.doc = self.doc[len(self.name) + 2:]
        if "==" in self.name:
            self.name, self.meta = self.name.split("==", 1)
        elif not self.name.startswith("--"):
            self.meta, self.name = self.name, self.meta

    def __repr__(self):
        return "StringDoc(%s, %s, %s)" % (self.name, self.meta, self.doc.replace("\n", "\\n"))

class Help(BaseException):

    pass

class OptionParser(object):

    def __init__(self, **kargs):
        self.options = []
        self.program = kargs.get("program", None)
        self.description = kargs.get("description", None)
        self.help_width=kargs.get("help_width", 80 - 24)
        self.scope = kargs.get("scope", globals())
        self.current_group = kargs.get("current_group", "")

    def add_option(self, match, **kargs):
        """
        Add option to the parser.

        'match' can be any Match object documented in the module, or
        just a string from '|' join-ed options.

        If 'func' key-argument provided, with the optional 'nary'
        argument(default 'nary' is calculated by inspect), the new
        index is old_index plus ('nary' + 1). the slice from old to
        new index is converted by convert(if exist), and passed to
        'func'.

        If 'func' and 'with_opt'=1, the slice also contains the option
        argument for matching(with '-' stripped).

        If 'func' and 'var'=1, the last arguments is gathered as a
        list until a argument starts with '-' encountered or to the
        end.

        If 'process' provided, it should follow the concept Process.

        If 'action' is provided, the value obtained from following
        argument, converted by 'convert'(or identity), is passed. If
        'with_opt'=1, the value is obtained from the matching
        argument, not the following one. If 'var'=1, the value is
        obtained from all continuous following not starts with '-'.

        If 'action' is not provided, the default action is assing
        'dest' in the 'scope'(or self.scope).

        If 'doc' is not provided, or the name(meta) part is missing,
        it will try match.name(process.meta) respectively.
        """
        if kargs.get("func", None):
            self._add_func_option(match, **kargs)
            return
        if kargs.get("process", None):
            self._add_gen_option(match, kargs["process"],
                                kargs.get("doc", ""),
                                kargs.get("group", self.current_group))
            return
        self._add_value_option(match, **kargs)

    def _add_gen_option(self, match, process, doc, group):
        """Add a very general option"""
        if group is None:
            group = self.current_group
        o = Option(match=match, process=process, doc=doc, group=group)
        self.options.append(o)

    def _add_func_option(self, match, **kargs):
        func = kargs.pop("func")
        doc = kargs.get("doc", "")
        process = FuncProcess(func, **kargs)
        self._add_gen_option(match, process, doc, kargs.get("group", self.current_group))

    def _add_value_option(self, match, **kargs):
        convert = kargs.pop("convert", None)
        scope = kargs.pop("scope", self.scope)
        dest = kargs.get("dest", None) or str(match).split("|")[-1].replace("-", "_").strip()
        action = kargs.pop("action", None)
        doc = kargs.pop("doc", "")
        if not action:
            dft = ""
            if dest in scope:
                dft = str(scope[dest])
            if convert is None:
                if dest in scope:
                    convert = type(scope[dest])
                if convert is None:
                    convert = str
            def dft_action(v):
                scope[dest] = convert(v)
            action = dft_action
            if dft and isinstance(doc, str):
                doc = (doc + "\ndefault '%s'" % dft).lstrip("\n")
        process = FuncProcess(action, **kargs)
        self._add_gen_option(match, process, doc, kargs.get("group", self.current_group))

    def _do_parse(self, idx, argv):
        self.program = len(argv) and argv[0]
        while idx < len(argv):
            matchs = []
            for i, o in enumerate(self.options):
                if isinstance(o.match, bool):
                    continue
                r = call_match(o.match, self, idx, argv)
                if r > MATCH_NONE:
                    matchs.append((r, i))
            matchs.sort(reverse=True)
            if not matchs:
                return idx
            if len(matchs) > 1:
                if matchs[0][0] == matchs[1][0]:
                    raise RuntimeError("option at %s('%s') with ambiguous match ..." % (idx, argv[idx]))
            new_idx = self.options[matchs[0][1]].process(self, idx, argv)
            if new_idx < 0:
                raise RuntimeError("option at %s('%s') failed ..." % (idx, argv[idx]))
            idx = new_idx
        return idx

    def parse_all(self, *argv_):
        """parse all arguments and print help then quit if not all parsed"""
        assert(len(argv_)) <= 2
        idx = 1
        if len(argv_) == 0:
            argv = sys.argv
        elif len(argv_) == 1:
            argv = argv_
        else:
            idx = argv_[0]
            argv = argv_[1]
        idx = self.parse(idx, argv)
        if idx != len(argv):
            self.help("parse stop at %s('%s') ..." % (idx, argv[idx]))

    def parse(self, *argv_):
        """parse arguments and return the last index not parsed"""
        assert(len(argv_)) <= 2
        idx = 1
        if len(argv_) == 0:
            argv = sys.argv
        elif len(argv_) == 1:
            argv = argv_
        else:
            idx = argv_[0]
            argv = argv_[1]
        try:
            idx = self._do_parse(idx, argv)
            return idx
        except Help:
            self.help("")
        except:
            traceback.print_exc()
            self.help("parse got exception")

    def help(self, mesg, **kargs):
        """print help message"""
        def split_doc(doc, width):
            off, pos = 0, len(doc)
            while off < len(doc) and doc[off].isspace(): off += 1
            if pos <= off + width: return (off, pos)
            pos = off + width
            while pos >= off + 1 :
                if doc[pos].isspace(): break
                pos -= 1
            if pos == off: # not breakable, search forward ...
                while pos < len(doc):
                    if doc[pos].isspace(): break
                    pos += 1
            return (off, pos)

        def split_lines(doc, width):
            lines = []
            while doc:
                lpos = doc.find("\n")
                if lpos == -1:
                    line = doc
                    doc = ""
                else:
                    line = doc[:lpos]
                    doc = doc[lpos + 1:]
                while True:
                    off, pos = split_doc(line, width)
                    sub = line[off:pos]
                    lines.append(sub)
                    line = line[pos:]
                    if not line: break
            return lines

        if mesg:
            _writeeln(mesg)
            _writeeln("")
        if self.program:
            _writeeln("Usage: %s [options] ..." % self.program)
        if self.description:
            _writeeln(self.description)
        _writeeln("")
        group_first = {}
        for i, o in enumerate(self.options):
            if not o.group in group_first:
                group_first[o.group] = i
        self.options.sort(key=lambda x:group_first[x.group])
        pre_group = None
        for o in self.options:
            if isinstance(o.doc, bool):
                continue
            if pre_group != o.group:
                if o.group:
                    _writeeln("\n%s:" % o.group)
                pre_group = o.group
            doc, pre = o.doc, ""
            doc, name, meta = doc.doc, doc.name, doc.meta
            pre = "\t\t\t"
            if meta:
                if len(name) < 23:
                    name = name + " " * (24 - len(name))
                    _writeeln("%s%s" % (name, meta))
                else:
                    _writeeln("%s\n%s%s" % (name, " " * 24, meta))
            else:
                _writeeln(name)
            lines = split_lines(doc.lstrip(), self.help_width);
            for l in lines:
                _writeeln("%s%s" % (pre, l))
                pre = "\t\t\t"
        c = kargs.get("exit", 1)
        if c >= 0:
            quit()

    def add_help(self, **kargs):
        """Add help option"""
        match = kargs.pop("match", "h|help")
        doc = kargs.pop("doc", "--" + str(match) + "::print help message")
        kargs["doc"] = doc
        def action():
            raise Help
        kargs["func"] = action
        self.add_option(match, **kargs)

    def parse_file(self, fp):
        """
        Parse options from file. Every line is splited with shlex, and
        the first item is prepended by '--', then passing to parse_all
        """
        if isinstance(fp, str):
            fp = open(fp, "rb")
        lines = [l.strip() for l in fp if l.strip() and not l.strip().startswith("#")]
        self.parse_lines(lines)

    def parse_lines(self, lines):
        """
        see parse_file
        """
        for l in lines:
            argv = shlex.split(l)
            if not argv: continue
            argv[0] = "--" + argv[0]
            self.parse_all(0, argv)

def to_date(s):
    """convert from string to date"""
    if not s:
        return datetime.date.today()
    if isinstance(s, datetime.datetime):
        return s.date()
    if isinstance(s, datetime.date):
        return s
    if isinstance(s, str) or isinstance(s, unicode):
        if s == 'today':
            return datetime.date.today()
        if s == 'yesterday':
            return datetime.date.today() - datetime.timedelta(days=1)
        if s.startswith('+') or s.startswith('-'):
            td = datetime.date.today()
            if s.endswith('y') or s.endswith('Y'):
                y, m, d = td.year - int(s[:-1]), td.month, td.day
                while True:
                    assert d > 0
                    try:
                        return datetime.date(y, m, d)
                    except:
                        d -= 1
            if s.endswith('m') or s.endswith('M'):
                y, m, d = td.year, td.month, td.day
                dm = int(s[:-1])
                y += (m + dm) / 12
                m = (m + dm) % 12
                while True:
                    assert d > 0
                    try:
                        return datetime.date(y, m, d)
                    except:
                        d -= 1
            if s.endswith('w') or s.endswith('W'):
                return td + datetime.timedelta(int(s[:-1]) * 7)
            if s.endswith('d') or s.endswith('D'):
                return td + datetime.timedelta(int(s[:-1]))
            return td + datetime.timedelta(int(s))
        return datetime.date(*[int(i) for i in s.replace('.', '-').split('-')[:3]])
    raise RuntimeError('to_date got date in invalid type %s:%s' %(type(s), s))
    return s

def to_datetime(dt):
    """convert from string to dattime"""
    if not dt:
        return datetime.datetime.now()
    if isinstance(dt, datetime.datetime):
        return dt
    if isinstance(dt, datetime.date):
        return datetime.datetime.fromtimestamp(time.mktime(dt.timetuple()))
    if isinstance(dt, str) or isinstance(dt, unicode):
        if dt == 'today' or dt == 'yesterday' or dt.startswith('+') or dt.startswith('-'):
            return to_datetime(to_date(dt))
        return datetime.datetime(*[int(i) for i in re.split('[^0-9]+', dt)])
    raise RuntimeError('to_datetime got date in invalid type %s:%s' %(type(date), date))
    return dt

if __name__ == "__main__":
    import re
    L = locals()
    cmd_parser = OptionParser(scope=L)
    style = "style"
    value = 10
    date = datetime.date.today()
    date_time = datetime.datetime.now()

    def opt_style_value(s, v):
        L["style"] = s
        L["value"] = int(v)
    def local_set(n, v):
        L[n] = v
    def local_bool(n):
        def func(v):
            L[n] = not "no-" in v
        return func
    ii = []
    cmd_parser.add_help()
    cmd_parser.add_option(
        match=dict(name="more help"),
        process=dict(meta="help meta"),
        doc="A virtual option, match none, just for more help message")
    cmd_parser.add_option(match=False, process=1, doc="and another more help message")
    cmd_parser.current_group = "single value"
    cmd_parser.add_option("s|style", doc="STYLE::set style")
    cmd_parser.add_option("v|value", doc="INT::set value")

    cmd_parser.current_group = "function with two parameter"
    cmd_parser.add_option("style-value", func=opt_style_value, doc="STYLE INT::set style and value")

    cmd_parser.current_group = "general match"
    cmd_parser.add_option(lambda p, idx, argv:(re.compile("^-+value-?[0-9]+$").match(argv[idx]) and MATCH_EXACT),
                          dest="value", with_opt=1,
                          convert=lambda x:int("".join(i for i in x if i.isdigit())),
                          doc="--value-NUM::inline option value")

    cmd_parser.current_group = "general func"
    cmd_parser.add_option("sum", func=lambda x, y:local_set("value", int(x) + int(y)),
                          doc="sum and set value")
    cmd_parser.add_option("config-file", func=lambda f:cmd_parser.parse_file(f),
                          doc="FILE::load configuration from FILE")
    cmd_parser.add_option("output|no-output", func=local_bool("output"), with_opt=1)

    cmd_parser.current_group = "var func"
    cmd_parser.add_option("ii", func=lambda ints:ii.extend(ints), var=1)
    cmd_parser.add_option("hidden-ii", func=lambda x:ii.extend(x), var=1, var_convert=int, doc=False)
    cmd_parser.add_option("date", convert=to_date, doc="DATE::give me a date")
    cmd_parser.add_option("date-time", convert=to_datetime, doc="DATETIME::give me a datetime")

    def print_options():
        for o in cmd_parser.options:
            _writeln(sys.stdout, o)
    cmd_parser.add_option("print-options", func=print_options, group="")
    def with_env(x):
        print "with_env", x
    cmd_parser.add_option("env", func=with_env, group="")
    cmd_parser.add_option(lambda p, idx, argv: (MATCH_EXACT if re.compile("^[^ \t]+=.*$").match(argv[idx]) else MATCH_NONE),
                          func=with_env, with_opt=1, group="")

    cmd_parser.parse_all()

    _writeln(sys.stdout, type(style), style)
    _writeln(sys.stdout, type(value), value)
    _writeln(sys.stdout, type(ii), ii)
    _writeln(sys.stdout, type(date), date)
    _writeln(sys.stdout, type(date_time), date_time)
