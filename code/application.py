import signal
import sys
import database


def help():
    msg = """help: display this message
exit: leave the program
precipitationVsNumIncidents
propertyDamageVsPrecipitation
precipitationByCity <city>
totalPrecipitationByState <state>
totalIncidentsByCity <city>"""
    print(msg)

def bye():
    print('bye.')
    sys.exit(0)


def sigint_handler(sig, frame):
    bye()


def precipitation_vs_num_incidents(db):
    # TODO
    pass


def property_damage_vs_precipitation(db):
    # TODO
    pass


def precipitation_by_city(db, city):
    # TODO
    pass


def total_precipitation_by_state(db, state):
    # TODO
    pass


def total_incidents_by_city(db, city):
    # TODO
    pass


def main():
    connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
    db = database.DBMSFinalProject(connection_string)
    print("Enter 'help' for a list of commands.")
    while True:
        user_input = input('> ')
        parts = user_input.split()

        if len(parts) == 0:
            print("Ill-formatted input. Enter 'help' for a list of commands.")

        command = parts[0]

        # command dispatch
        if command == 'help':
            help()
        elif command == 'exit':
            bye()
        elif command == 'precipitationVsNumIncidents':
            if len(parts) != 1:
                print("Usage: precipitationVsNumIncidents")
            else:
                precipitation_vs_num_incidents(db)
        elif command == 'propertyDamageVsPrecipitation':
            if len(parts) != 1:
                print("Usage: propertyDamageVsPrecipitation")
            else:
                property_damage_vs_precipitation(db)
        elif command == 'precipitationByCity':
            if len(parts) != 2:
                print("Usage: precipitationByCity <city>")
            else:
                city = parts[1]
                precipitation_by_city(db, city)
        elif command == 'totalPrecipitationByState':
            if len(parts) != 2:
                print("Usage: totalPrecipitationByState <state>")
            else:
                state = parts[1]
                total_precipitation_by_state(db, state)
        elif command == 'totalIncidentsByCity':
            if len(parts) != 2:
                print("Usage: totalIncidentsByCity <city>")
            else:
                city = parts[1]
                total_incidents_by_city(db, city)
        else:
            print("Unrecognized command. Enter 'help' for a list of commands.")


if __name__ == '__main__':
    # register sigint handler
    signal.signal(signal.SIGINT, sigint_handler)
    main()
