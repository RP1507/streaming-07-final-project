"""
    This program will listen continuously from Canvas to capture mid term grades being entered by professors.
    It will capture the letter grade, and the associated gpa with that letter
    An alert will trigger for grades lower than a D or for gpas that dip below 2.0

    Author: Ryan Smith
    Date: 10/06/2023

"""

import pika
import sys
import logging
from collections import deque
import datetime

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define global variables for grade monitoring
GRADED_LETTERS = deque(maxlen=int(2.5 / 0.5))  # 2.5 minute window for smoker grades read every 30 seconds
GRADED_GPA = deque(maxlen=int(10 / 0.5))    # 10 minute window over the 30 second intervals. 

LETTER_ALERT = "D"  #  grade limit threshold letter
GPA_ALERT = 2.0    #  grade change threshold for gpa

def process_grades(body, grade_name, grade_deque, alert_threshold):
    try:
        # break up the message sent from the producer file into 2 strings
        student_str, grade_str = body.decode().split(',')
        #print(student_str) - testing
        # convert the student string into a datetime object'
        student_str = student_str.replace('[', '')
        grade_str = grade_str.replace(']','')
        #print(student_str) - testing to make sure brackets were removed
        #print(grade_str) - testing for bracket removal
        student = student_str #datetime.datetime.strptime(student_str, '%m/%d/%Y %H:%M')
        print(student)
       

        # Strip leading and trailing whitespace, handle 'None' values, and convert to float
        # Added this as I was having trouble with None values
        grade = (grade_str.strip()) if grade_str.strip().lower() != 'none' else None
        print(grade)
        # Add student and recorded grade to deque
        grade_deque.append((student, grade))

        # Check for alert condition
        if len(grade_deque) == grade_deque.maxlen:
            # Handle None values that may occur
            valid_grades = [grade[1] for grade in grade_deque if grade[1] is not None]

            # Handle missing grade values
            if len(valid_grades) < 2:
                logging.warning(f"Not enough valid grades for {grade_name} callback.")
                return
            # Record grade values and the difference in time between them from the deque
            first_grade = valid_grades[0]
            last_grade = valid_grades[-1]
            #time_diff = (grade_deque[-1][0] - grade_deque[0][0]).total_seconds() / 60.0  # Convert to minutes
            # initial grade and time reading
            logging.info(f"{grade_name} - First Grade: {first_grade}, Second Grade: {last_grade}, Grade Difference")
            # if grade and time change threshold is passed, log the event
            if grade == 'D': # >= alert_threshold and time_diff <= get_time_window(grade_name):
                logging.info(f"{grade_name} Alert: Low grade: >= {alert_threshold}D or below.")

    except Exception as e:
        logging.error(f"Error in {grade_name} callback: {e}")
""""
def get_time_window(grade_name):
    if grade_name == "GRADE_CDE":
        return 2.5  # Smoker time window is 2.5 minutes
    elif grade_name == "CRS_GPA":
        return 10.0   # Food time window is 10 minutes
    elif grade_name == "Food B":
        return 10.0   # Food time window is 10 minutes
    else:
        return None
"""
def lettergrade_callback(ch, method, properties, body):
    process_grades(body, ":Student Grade", GRADED_LETTERS, LETTER_ALERT)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def gpa_callback(ch, method, properties, body):
    process_grades(body, "Student GPA", GRADED_GPA, GPA_ALERT)
    ch.basic_ack(delivery_tag=method.delivery_tag)

#def food_b_callback(ch, method, properties, body):
 #   process_grades(body, "Food B", FOOD_gradeS, FOOD_STALL)
 #   ch.basic_ack(delivery_tag=method.delivery_tag)

def main(hn: str = "localhost"):
    """ Continuously listen for task messages on multiple named queues."""

    # create a list of queues and their associated callback functions
    queues_and_callbacks = [
        ("letter_grade", lettergrade_callback),
        ("course_gpa", gpa_callback),
        #("03-food-B", food_b_callback)
    ]

    try:
        # Modify this line to include your RabbitMQ username and password
        credentials = pika.PlainCredentials('guest', 'guest')

        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=hn, credentials=credentials)
        )

        # use the connection to create a communication channel
        channel = connection.channel()

        for queue_name, callback_function in queues_and_callbacks:
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            channel.queue_declare(queue=queue_name, durable=True)

            # The QoS level controls the # of messages
            # that can be in-flight (unacknowledged by the consumer)
            # at any given time.
            # Set the prefetch count to one to limit the number of messages
            # being consumed and processed concurrently.
            # This helps prevent a worker from becoming overwhelmed
            # and improve the overall system performance.
            # prefetch_count = Per consumer limit of unacknowledged messages
            channel.basic_qos(prefetch_count=1)

            # configure the channel to listen on a specific queue,
            # use the appropriate callback function,
            # and do not auto-acknowledge the message (let the callback handle it)
            channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=False)

        # log a message for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")