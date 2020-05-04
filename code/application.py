import signal
import sys
import database
import shlex
import cmd
import functools

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"


def bye():
    """Exit politely"""
    print('bye.')
    sys.exit(0)


def sigint_handler(sig, frame):
    bye()


def help_on_bad(f):
    """Wrap a command that returns a bool. If f returns false, print an error message and the documentation for f."""
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        res = f(self, *args, **kwargs)
        if not res:
            print("Input not understood.")
            self.do_help(f.__name__.split("_")[1])

    return wrapper


class Shell(cmd.Cmd):
    """Interprets user input."""
    intro = "Enter 'help' for a list of commands."
    prompt = "> "

    def __init__(self, connection_string):
        super().__init__()
        self.db = database.DBMSFinalProject(connection_string)

    @help_on_bad
    def do_precipitationVsNumIncidents(self, arg):
        args = parse(arg)
        if len(args) > 0:
            return False

        res = self.db.precipitation_vs_num_incidents()
        # TODO
        print(res)

        return True

    @help_on_bad
    def do_propertyDamageVsPrecipitation(self, arg):
        """Get a table of property loss and precipitation: propertyDamageVsPrecipitation"""
        args = parse(arg)
        if len(args) > 0:
            return False

        res = self.db.property_damage_vs_precipitation()
        # TODO
        print(res)

        return True

    @help_on_bad
    def do_precipitationByCity(self, arg):
        """How much precipitation was there in a city: precipitationByCity 'Juneau, AK'"""
        args = parse(arg)
        if len(args) == 0:
            return False

        location = args[0]

        # parse city, state
        parts = location.split(',')
        if len(parts) != 2:
            return False

        city, state = parts
        city = city.strip()
        state = state.strip()

        precip = self.db.precipitation_by_city(city, state)
        # TODO
        print(precip)

        return True

    @help_on_bad
    def do_totalPrecipitationByState(self, arg):
        args = parse(arg)
        if len(args) > 0:
            return False

        res = self.db.total_precipitation_by_state()
        # TODO
        print(res)

        return True

    @help_on_bad
    def do_totalIncidentsByCity(self, arg):
        """How many incidents have there been in a city: totalIncidentsByCity 'New York City, NY'"""
        args = parse(arg)
        if len(args) == 0:
            return False

        location = args[0]

        parts = location.split(',')
        if len(parts) != 2:
            return False

        city, state = parts
        city = city.strip()
        state = state.strip()

        res = self.db.total_incidents_by_city(city, state)
        # TODO
        print(res)

        return True

    @help_on_bad
    def do_exit(self, arg):
        """Exit the program: exit"""
        if len(parse(arg)) > 0:
            return False

        bye()

    def do_EOF(self, arg):
        return True


def parse(arg):
    """Parse command input into a list of elements. Quoted values are handled as whole parts of the command."""
    return shlex.split(arg)


if __name__ == '__main__':
    # register sigint handler
    signal.signal(signal.SIGINT, sigint_handler)
    Shell(connection_string).cmdloop()
