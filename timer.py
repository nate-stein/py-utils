"""
Multi-purpose execution log to track program process and timing.
"""

import time

from pandas import DataFrame


#####################################################################
class Event:
    """Data container used by ProgramTimer to err_log events.

    Attributes:
        name (str): Description of event.
        uid (int): Unique ID for event.
        hrchy (int): Number of chained parent events. Default is 1 (i.e.,
        no parent events).
        pid (int): uid of parent event; if None, class assumes there is no
        parent event.
    """

    def __init__ (self, name, uid, hrchy=1, pid=None):
        self.name = name
        self.uid = uid
        self.hrchy = hrchy
        self.pid = pid
        self.__start = time.time()
        self.__end = None

    def end (self):
        """Sets e time for event."""
        self.__end = time.time()

    def is_open (self):
        """Returns True if event has not been terminated."""
        if self.__end is None:
            return True
        return False

    def seconds (self):
        """Returns seconds the event has currently been open (for current event)
        or total seconds (for closed event).
        """
        if self.__end is None:
            return time.time() - self.__start
        return self.__end - self.__start

    def __str__ (self):
        return self.name + ': ' + "{0:.2f}".format(self.seconds()) + ' seconds'


#####################################################################
class ProgramTimer:
    """Catch-all program execution log, keeping timing of general tasks and
    logging exceptions for data-oriented programs.
    
    Attributes:
        events (dict[int, Event]): Keeps internal dict object of all `Event`s
        where their IDs are commensurate with the order in which they were
        opened i.e. id number (k+1) is the event opened immediately after the
        event with id number k.
        update_on_start (bool): Optional, default True. If True, update will be
        printed when new event is started.
        update_on_end (bool): Optional, default False. If True, update will be
        printed when event is closed.
        header (str): Appears as header when summary is printed.
        errors (DataFrame): Calls to log_err() are stored here.
    """

    def __init__ (self, ud_start=True, ud_end=False, **kwargs):
        self.events = {}
        self.update_on_start = ud_start
        self.update_on_end = ud_end
        self.header = kwargs.get('header', 'Execution Summary')
        self.errors = DataFrame(columns=['method', 'data_id', 'err_info'])

        # Default strings going before and after an event name when we want
        # to print an update upon starting an event.
        self._start_ud_pre_txt = kwargs.get('start_txt', 'Started: ')
        self._start_ud_post_txt = '...'
        self._end_ud_pre_txt = 'Finished: '
        self._end_ud_post_txt = '.'

    def s (self, name):
        """Starts event."""
        new_id = self.__event_count + 1
        prior_open_id = self.__prior_open_event_id()
        if prior_open_id is None:
            hrchy = 1
        else:
            hrchy = self.events[prior_open_id].hrchy + 1

        event = Event(name, new_id, hrchy, prior_open_id)
        self.events[new_id] = event
        if self.update_on_start:
            msg = self._start_ud_pre_txt + name + \
                  self._start_ud_post_txt
            msg = '  '*(hrchy - 1) + msg
            print(msg)

    def e (self, ud=None, msg=None, n=1):
        """Terminates event and prints message if warranted.

        Args:
            ud (bool): Optional. If set, overrides self.update_on_end for
            this event.
            msg (str): Optional. If provided, it is assumed user wants to
            output the message.
            n (int): Optional, default 1. Number of layers to terminate. If,
            for example, one wanted to terminate two opened events, one would
            pass n=2.
        """
        if msg is not None:
            ud = True

        prior_open_id = self.__prior_open_event_id()
        if prior_open_id is None:
            raise Exception(
                  'ProgramTimer.e() method called when there are no prior '
                  'open events.')
        else:
            self.events[prior_open_id].end()
        # Print update if warranted.
        if self.update_on_end or ud:
            if msg is None:
                event = self.events[prior_open_id].name
                msg = self._end_ud_pre_txt + event + self._end_ud_post_txt
            print(msg)
        if n>1:
            self.e(ud, msg, n - 1)

    def __prior_open_event_id (self):
        """Returns uid of the closest prior Event that is still open; None if
        there are no prior events still open.
        """
        if self.__event_count==0:
            return None

        for i in range(self.__event_count, 0, -1):
            if self.events[i].is_open():
                return self.events[i].uid
        return None

    @property
    def __event_count (self):
        return len(self.events)

    def print_summary (self, header=None):
        """Prints two sections:
        (a) summary of events with spaces to indicate hierarchy and
        (b) the DF storing exceptions.

        Args:
            header (str): Optional. Overrides header attr if provided.
        """
        SECTION_DIVIDE = '-'*40
        # Build main program execution section.
        summary = '{0}{1}{2}'.format('\n', SECTION_DIVIDE, '\n')
        if header is None:
            header = self.header

        summary = summary + header + '\n\n'
        for i in range(1, self.__event_count + 1, 1):
            event_details = self.__create_event_summary(self.events[i])
            summary = summary + event_details + '\n'

        # Add separate Exceptions section only if errors were encountered.
        if self.errors_were_logged:
            summary += '{}{}'.format(SECTION_DIVIDE, '\n')
            summary += 'Exceptions\n\n'
            summary += self.errors.to_string(index=False)
        else:
            summary += '\nNo errors logged.'

        summary += '{0}{1}{2}'.format('\n', SECTION_DIVIDE, '\n')
        print(summary)

    @staticmethod
    def __create_event_summary (event):
        """Returns str summary of Event; only used by print_summary() method."""
        blank_space = (event.hrchy - 1)*'   '
        summary = blank_space + event.name
        summary += ': ' + '{0:.1f}'.format(event.seconds()) + 's'
        return summary

    def log_err (self, method, data_id, info):
        new_err = {'method':method, 'data_id':data_id, 'err_info':info}
        self.errors = self.errors.append(new_err, ignore_index=True)

    @property
    def errors_were_logged (self) -> bool:
        return not self.errors.empty


def time_func (f):
    """Decorator to print elapsed CPU time for function call."""
    name = f.__name__

    def wrap (*args, **kwargs):
        start = time.clock()
        result = f(*args, **kwargs)
        t = time.clock() - start
        print('{} elapsed time: {:.0f}s'.format(name, t))
        return result

    return wrap


def time_method (f):
    """Decorator to print elapsed CPU time for method call."""
    name = f.__name__

    def wrap (*args, **kwargs):
        start = time.clock()
        f(*args, **kwargs)
        t = time.clock() - start
        print('{} elapsed time: {:.0f}s'.format(name, t))

    return wrap
