import signal
import sys
import database
import shlex
import cmd
import functools
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option('display.max_rows',200)
pd.set_option('display.min_rows', 100)

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
        """\n----------------------------------------------\nGet a table of and precipitation and incidents\n\nUSAGE: precipitationVsNumIncidents\nTo display all results, run with flag --all\n----------------------------------------------\n"""
        print_all = False

        args = parse(arg)
        if len(args) > 0:
            flag = args[0].strip()
            if flag == '--all':
                print_all = True
            else:
                return False

        res = self.db.precipitation_vs_num_incidents()

        zipcodes = []
        incidents = []
        precips = []

        for line in res:
            zipcodes.append(line[0])
            incidents.append(line[1])
            precips.append(line[2])

        data = {'Zipcode':zipcodes, 'Number of Incidents':incidents, 'Precipitation (Inches)':precips}
        df = pd.DataFrame(data)

        blank_index = [''] * len(df)
        df.index = blank_index

        if(print_all):
            pd.set_option('display.max_rows',None)
        else:
            pd.set_option('display.max_rows',200)

        # print(df)

        print('\n',df,'\n')

        plt.scatter(precips, incidents, s=10)
        plt.xlabel('Precipitation (Inches)')
        plt.ylabel('Number of Incidents')
        plt.suptitle('Precipitation VS Number of Incidents')
        # plt.show()

        plt.savefig('precipitation_vs_num_incidents.png')


        return True

    @help_on_bad
    def do_propertyDamageVsPrecipitation(self, arg):
        """\n----------------------------------------------\nGet a table of property loss and precipitation\n\nUSAGE: propertyDamageVsPrecipitation\nTo display all results, run with flag --all\n----------------------------------------------\n"""
        print_all = False

        args = parse(arg)
        if len(args) > 0:
            flag = args[0].strip()
            if flag == '--all':
                print_all = True
            else:
                return False

        res = self.db.property_damage_vs_precipitation()
        
        damage = []
        precips = []

        for line in res:
            damage.append(line[0])
            precips.append(line[1])

        data = {'Property Loss':damage, 'Precipitation (Inches)':precips}
        df = pd.DataFrame(data)

        blank_index = [''] * len(df)
        df.index = blank_index

        if(print_all):
            pd.set_option('display.max_rows',None)
        else:
            pd.set_option('display.max_rows',200)

        print('\n',df,'\n')

        plt.scatter(precips, damage, s=10)
        plt.xlabel('Precipitation (Inches)')
        plt.ylabel('Property Loss')
        plt.suptitle('Property Damage VS Precipitation')
        # plt.show()

        plt.savefig('property_damage_vs_precipitation.png')

        return True

    @help_on_bad
    def do_precipitationByCity(self, arg):
        """\n------------------------------------------\nHow much precipitation was there in a city\n\nUSAGE: precipitationByCity 'Juneau, AK\n------------------------------------------\n'"""
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

        print('\n',precip[0], "inches\n")

        return True

    @help_on_bad
    def do_totalPrecipitationByState(self, arg):
        """\n-------------------------------------------------------------\nGet a table of how much precipitation there was in each state\n\nUSAGE: totalPrecipitationByState\n-------------------------------------------------------------\n"""
        args = parse(arg)
        if len(args) > 0:
            return False

        res = self.db.total_precipitation_by_state()

        state = []
        precips = []

        for line in res:
            state.append(line[0])
            precips.append(line[1])

        data = {'State':state, 'Precipitation (Inches)':precips}
        df = pd.DataFrame(data)

        blank_index = [''] * len(df)
        df.index = blank_index
        # print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        # print(df)

        print('\n',df,'\n')

        return True

    @help_on_bad
    def do_totalIncidentsByCity(self, arg):
        """\n--------------------------------------------\nHow many incidents have there been in a city\n\nUSAGE: totalIncidentsByCity 'New York City, NY\n--------------------------------------------\n'"""
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
        print('\n',res[0][0], "incidents\n")

        return True

    @help_on_bad
    def do_exit(self, arg):
        """\n----------------\nExit the program\n\nUSAGE: exit\n----------------\n"""
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
