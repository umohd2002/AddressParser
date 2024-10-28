# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 18:22:53 2023

@author: Salman Khan
"""
from flask_socketio import SocketIO, emit
import time  # Used for simulating processing time
from flask import Flask, request, render_template, jsonify, send_file, session, send_from_directory, Response, stream_with_context
from functools import wraps
from sqlalchemy import create_engine, func, distinct, update
from sqlalchemy.orm import sessionmaker
from flask import Flask, render_template, redirect, request, url_for, flash
from werkzeug.utils import secure_filename, safe_join
from tqdm import tqdm
from flask_socketio import SocketIO
import os
from ORM import MaskTable, ComponentTable, MappingJSON, User, UserRole, ExceptionTable, MapCreationTable
from DB_Operations import DB_Operations as CRUD
import SingleAddressParser_Module as SAP
from werkzeug.security import generate_password_hash, check_password_hash
import hashlib
import Address_Parser__Module as BAP
from flask_cors import CORS
import json
import threading
from datetime import datetime
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
# from LoginORM import User, UserRole
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import InputRequired, Length
from flask_socketio import SocketIO, emit
import logging
import bcrypt
import base64
from flask_session import Session as sess
from datetime import timedelta
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
logging.basicConfig(level=logging.DEBUG)

current_time = datetime.now()
app = Flask(__name__, template_folder='templates')
app.config['SESSION_TYPE'] = 'filesystem'  # Can be 'redis', 'memcached', etc.
sess(app)





app.permanent_session_lifetime = timedelta(days=7)
engine = create_engine('sqlite:///KnowledgeBase.db',echo=True)
# engine2 = create_engine('sqlite:///KnowledgeBase.db', echo=True)
Session = sessionmaker(bind=engine)
# DBSession = sessionmaker(bind=engine2)
original_secret_key = 'Parser_secret!'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///KnowledgeBase.db?check_same_thread=False'

# Hash the original secret key using SHA256
hashed_secret_key = hashlib.sha256(original_secret_key.encode()).hexdigest()

# Assign the hashed value to secret_key
secret_key = hashed_secret_key
app.config['SECRET_KEY'] = hashed_secret_key
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024  # for 256MB max- file size

# socketio = SocketIO(app)
CORS(app)
# login_manager = LoginManager()
# login_manager.init_app(app)
# login_manager.login_view = 'login'

# class LoginForm(FlaskForm):
#     username = StringField('Username', validators=[InputRequired(), Length(min=4, max=30)])
#     password = PasswordField('Password', validators=[InputRequired()])
#     submit = SubmitField('Login')

class RegisterForm(FlaskForm):
    username = StringField('Username', validators=[InputRequired(), Length(min=4, max=30)])
    password = PasswordField('Password', validators=[InputRequired()])
    submit = SubmitField('Register')

class BatchUploadForm(FlaskForm):
    file = FileField('Upload File', validators=[FileRequired()])
    submit = SubmitField('Process File')

def hash_password(password):
    """ Hash a password using bcrypt """
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt)

# @app.route('/login', methods=['GET', 'POST'])
# def login(): 
#     form = LoginForm()  # Ensure this matches the name of your form class
#     if form.validate_on_submit():
#             sessions = Session()
#             username = request.form['username']
#             password = request.form['password']  
#             user = sessions.query(User).filter_by(UserName=username).first()
#             # userTable = sessions.query(User).all()
#             if user and bcrypt.checkpw(password.encode(), user.Password): 
#                 role_name = user.role.RoleName
                
#                 session["user_id"]=username
#                 session["role"]= role_name
#                 # session['status'] = user.Status
#                 session["FullName"] = user.FullName
#                 # print("\n\n\n",session['status'],"\n", user.Status,"\n\n\n")
#                 return redirect(url_for('SingleLineAddressParser')) 
            
#            # Remember, in real apps, don't use plain text for passwords
           
#             else:
#                 session.clear()
#                 flash('Invalid username or password')
#     return render_template('login.html', form=form)

# @login_manager.user_loader
# def load_user(user_id):
#     return User.query.get(int(user_id))


# def requires_role(role):
#     def decorator(f):
#         @wraps(f)
#         def decorated_function(*args, **kwargs):
#             if 'user_id' not in session:
#                 flash('You need to be logged in to view this page.')
#                 return redirect(url_for('authentication'))
#             user = session['user_id']
#             role= session["role"]
#             if not user or role != role:
#                 flash('You do not have the required permissions to view this page.')
#                 return redirect(url_for('SingleLineAddressParser'))  # Or some other appropriate redirect
#             return f(*args, **kwargs)
#         return decorated_function
#     return decorator



# @app.route('/logout', methods=["GET", "POST"])
# def logout():
#     flash('You have been logged out!')
#     session.clear()
#     return redirect(url_for('login'))






@app.route('/', methods=["GET", "POST"])
# @requires_role('Admin')
def SingleLineAddressParser():
    result = {}
    form = BatchUploadForm()
    try:
        
        if request.method == 'POST':
            address = request.form['address']
            convert = SAP.Address_Parser(address, address)
            if convert[4]:
                result = convert[0]
                result['Parsed_By'] = 'Rule Based'
                # print("result: ", result)
            else:
                result = convert[0]
                result['Parsed_By'] = 'Active Learning'
                # print("result: ", result)
            return jsonify(result=result)
    except:
        return jsonify('index.html', result=result, form=form)
            
    return render_template('index.html', result=result, form=form)


@app.route("/forceException", methods=["GET", "POST"])
def forceException():
    response = {'result': False}
    global download_except_path  # Make sure to use the global variable
    if request.method == "POST":
        address = request.form["address"]
        convert = SAP.throwException(address)
        mapdata = {}
        excdata = {}
        mapdata["Address Input"] = address
        excdata["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        rules = convert[1][0]
        # excdata["Username"] = session["user_id"]
        excdata["Username"] = "admin"
        excdata["Run"] = "Single"
        excdata["Record ID"] = rules["Record ID"]
        mapdata["Mask"] = next((key for key, value in rules.items() if isinstance(value, list)), None)
        # print(mask)
        excdata["data"] = rules[mapdata["Mask"]]
        # print("excdata: ", excdata)

        # print(mapdata)
        CRUD.add_mapCreation(engine,mapdata, excdata)


        # if convert is not None:  # Check if the return value is not None
        #     response['result'] = True
        #     download_except_path = convert[0]  # Assign the returned file path
        # print("RuleBase: ", convert[1])

        
    return jsonify(response=response) #, download_url="/download_except")

# @app.route('/download_except')
# def download_except_file():  # Ensure this function name is unique
#     global download_except_path
#     if download_except_path is None:
#         return jsonify({'error': 'No file to download'}), 404
#     try:
#         return send_file(download_except_path, as_attachment=True ,mimetype='application/json')
#     except FileNotFoundError:
#         return jsonify({'error': 'File not found'}), 404


task_results = {}
def process_file_in_background(file, filename):
    convert = BAP.Address_Parser(file, "update_progress")
    task_results[filename] = {
        "result": convert[1] if convert[0] else None,
        "metrics": {'metrics': convert[1]} if convert[0] else None,
        "output_file_path": convert[2] if convert[0] else None
    }
    
    with open("temp_file.json", "w", encoding = "utf8") as file:
        json.dump(task_results, file, indent=4)
        
        


@app.route('/Batch_Parser', methods=["GET", "POST"])
def BatchParser():
    form = BatchUploadForm()
    # print("Processing starsted: ", flush=True)
    
    if form.validate_on_submit():
        global task_results

        file = form.file.data
        filename = secure_filename(file.filename)
        file_path = os.path.join('File Uploads', filename)
        file.save(file_path)

        thread = threading.Thread(target=process_file_in_background, args=(file_path, filename))
        thread.run()
        if os.path.exists(file_path):
            os.remove(file_path)
       
        return jsonify(status="Processing started", status_check_url='/check_status/' + filename, download_url='/download_output/' + filename)

    return jsonify(status="Upload a file")

@app.route('/check_status/<filename>', methods=["GET", "POST"])
def check_status(filename):
    # print("sdksdksd") 
    task_results=dict()
    try:
            
        with open("temp_file.json", "r", encoding="utf8") as file:
        # Load the JSON content from the file into a Python dictionary
            task_results = json.load(file)
        if "result" in task_results[filename] and task_results[filename]["result"] and filename in task_results is not None:
            return jsonify(result=task_results[filename]["result"], metrics=task_results[filename]["metrics"], output_file_path = task_results[filename]["output_file_path"])
        else:
            return jsonify(status="Still processing"), 202
    except:
        pass
# else:
    #return jsonify(error=str(task_results)), 404

@app.route('/download_output/<filename>')
def download_file(filename):
    task_results=dict()
    with open("temp_file.json", "r", encoding="utf8") as file:
    # Load the JSON content from the file into a Python dictionary
        task_results = json.load(file)

    if filename in task_results and task_results[filename]["output_file_path"] is not None:
        try:
            return send_file(task_results[filename]["output_file_path"], as_attachment=True)
        except FileNotFoundError:
            return jsonify({'error': 'File not found'}), 404
    else:
        return jsonify({'error': 'Result not ready or file not found'}), 404

@app.route('/removefile', methods=['POST'])
def remove_file():
    try:
        data = request.json  # More idiomatic way to handle JSON data
        output_file_path = data.get('output_file_path')
        # print("Output Path", output_file_path)

        full_path = os.path.join(app.root_path, output_file_path)  # Adjust if necessary

        if os.path.exists(full_path):
            os.remove(full_path)
            return jsonify({'status': 'success', 'message': 'File removed successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'File not found'}), 404

    except Exception as e:
        print(f"Error occurred: {e}")  # Log the actual error message
        return jsonify({'status': 'error', 'message': 'An internal error occurred'}), 500


@app.route('/get_runs')
def get_runs():
    try:
        session = Session()
        # Query distinct Run values from ExceptionTable
        runs = session.query(ExceptionTable.Run).distinct().all()
        # Return the runs as JSON response
        return jsonify([run[0] for run in runs])
    except Exception as e:
        # Log the error and return an appropriate error response
        app.logger.error(f"Error fetching runs: {e}")
        return jsonify({"error": "An error occurred while fetching runs"}), 500
    finally:
        # Ensure the session is closed after use
        session.close()


@app.route('/get_users/<run>')
def get_users(run):
    try:
        session = Session()
        # Query distinct UserName values based on the given Run
        users = session.query(ExceptionTable.UserName).filter_by(Run=run).distinct().all()
        # Return the usernames as JSON response
        return jsonify([user[0] for user in users])
    except Exception as e:
        # Log the error and return an appropriate error response
        app.logger.error(f"Error fetching users for run {run}: {e}")
        return jsonify({"error": f"An error occurred while fetching users for run {run}"}), 500
    finally:
        # Ensure the session is closed after use
        session.close()


@app.route('/get_timestamps/<run>/<user>')
def get_timestamps(run, user):
    try:
        session = Session()
        # Query distinct Timestamp values based on Run and UserName
        timestamps = session.query(ExceptionTable.Timestamp).filter_by(Run=run, UserName=user).distinct().all()
        # Return the timestamps as JSON response
        return jsonify([timestamp[0] for timestamp in timestamps])
    except Exception as e:
        # Log the error and return an appropriate error response
        app.logger.error(f"Error fetching timestamps for run {run} and user {user}: {e}")
        return jsonify({"error": f"An error occurred while fetching timestamps for run {run} and user {user}"}), 500
    finally:
        # Ensure the session is closed after use
        session.close()


@app.route('/process_dropdown_data', methods=['POST'])
def process_dropdown_data():
    session = Session()
    try:
        data = request.json
        run = data.get('run')
        user = "admin"  # You can switch this to: data.get('user') if it should be dynamic
        timestamp = data.get('timestamp')
        print(f"\n\n\n\nTimeStamp Before first Query {datetime.now()}\n\n\n\n")
        Ids = (
            session.query(ExceptionTable.Address_ID)
            .filter(
                ExceptionTable.Run == run,
                ExceptionTable.UserName == user,
                ExceptionTable.Timestamp == timestamp
            )
            .distinct(ExceptionTable.Address_ID)
            .limit(50)
            .all()
        )

        address_id_list = [id_tuple[0] for id_tuple in Ids]
        print(f"\n\n\n\nTimeStamp Before second Query {datetime.now()}\n\n\n\n")
        # page = data.get("page", 1)
        # per_page = 20
        exception_dict = session.query(
                ExceptionTable.Address_ID,
                MapCreationTable.Address_Input,
                MapCreationTable.Mask,
                ExceptionTable.Component,
                ExceptionTable.Mask_Token,
                ExceptionTable.Token,
                ExceptionTable.Component_index,
                ComponentTable.description
            ).join(
                MapCreationTable, ExceptionTable.MapCreation_Index == MapCreationTable.ID
            ).join(
                ComponentTable, ExceptionTable.Component == ComponentTable.component
            ).filter(
                # ExceptionTable.Run == run,
                # ExceptionTable.UserName == user,
                ExceptionTable.Timestamp == timestamp,
                ExceptionTable.Address_ID.in_(address_id_list)
            ).order_by(
                ExceptionTable.Address_ID,
                ExceptionTable.Component_index
            ).all()
        # .offset((page - 1) * per_page).limit(per_page).all()

        print(f"\n\n\n\nTimeStamp Before third Query {datetime.now()}\n\n\n\n")
        # Query 3: Get the total count of distinct Address_IDs
        total_dict = session.query(
            func.count(distinct(ExceptionTable.Address_ID))
            ).filter(
                ExceptionTable.Run == run, 
                ExceptionTable.UserName == user, 
                ExceptionTable.Timestamp == timestamp
            ).scalar()
        print(f"\n\n\n\nTimeStamp After third Query {datetime.now()}\n\n\n\n")
        # Process the query data
        data = process_query_data(exception_dict)

        # Return the processed data and the total count of dictionaries
        return jsonify({"status": "success", "message": "Data processed", "data": data, "total_dict": total_dict})

    except Exception as e:
        app.logger.error(f"Error processing dropdown data: {e}")
        return jsonify({"status": "error", "message": "An error occurred while processing the data"}), 500

    finally:
        session.close()


def process_query_data(query_data):
    processed = []
    current_record_id = None
    current_dict = {}
    dynamic_key_list = None
    dynamic_key = None

    for record in query_data:
        record_id, input_address, mask, component, mask_token, token, _, description = record

        # When a new record ID is encountered
        if record_id != current_record_id:
            # Save the previous record's data if it exists
            if current_dict:
                current_dict[dynamic_key] = dynamic_key_list  # Add the last dynamic key's list to the dictionary
                processed.append(current_dict)  # Append the completed record to the processed list

            # Initialize a new record
            current_record_id = record_id
            current_dict = {"Record ID": str(record_id), "INPUT": input_address}  # Create a new dictionary for the record
            dynamic_key = mask  # Set the new dynamic key based on the mask
            dynamic_key_list = []  # Initialize the dynamic key list

        # When the mask changes within the same record ID, store the previous mask's data
        if mask != dynamic_key:
            current_dict[dynamic_key] = dynamic_key_list  # Store the current dynamic key list
            dynamic_key = mask  # Update to the new mask
            dynamic_key_list = []  # Reset the dynamic key list

        # Add the entry to the dynamic key list (for the current mask)
        nwftn_entry = [token, component, mask_token, description]
        dynamic_key_list.append(nwftn_entry)

    # After the loop, add the last record to the processed list
    if current_dict:
        current_dict[dynamic_key] = dynamic_key_list  # Add the last dynamic key list
        processed.append(current_dict)  # Append the last record to the processed list
    print(f"\n\n\n\nDictionary to send: {processed}\n\n\n\n")
    return processed



@app.route('/AddressComponents_dropdown', methods=['GET'])
def get_address_components():
    session = Session()
    try:
        # Query to get all component descriptions
        components = (
            session.query(ComponentTable.description)
            .filter(ComponentTable.description != 'Not Selected')
            .order_by(ComponentTable.description.asc())
            .all()
        )
        options = [component[0] for component in components]
        options.append('Not Selected')
        return jsonify(options)
    except Exception as e:
        app.logger.error(f"Error occurred while fetching address components: {e}")
        return jsonify({'error': 'An error occurred while fetching address components'}), 500
    finally:
        # Ensure session is closed
        session.close()


@app.route('/check-mask-existence', methods=['POST'])
def check_mask_existence():
    session = Session()
    try:
        data = request.get_json()
        mask = data.get('mask')
        
        # Query to check if the mask exists in the MaskTable
        mask_record = session.query(MaskTable).filter_by(mask=mask).first()
        
        # Return the result as JSON
        return jsonify({'exists': mask_record is not None})
    
    except Exception as e:
        app.logger.error(f"Error occurred while checking mask existence: {e}")
        return jsonify({'error': 'An error occurred while checking mask existence'}), 500
    
    finally:
        # Ensure the session is closed
        session.close()



@app.route('/MapCreationForm-Data', methods=["GET", "POST"])
def MapCreationForm():
    db_session = Session()
    try:
        result = {}
        mapdata = request.get_json()

        # Using hardcoded username for now, replace with session user when available
        username = "admin"
        Address_ID = mapdata.get("Record Id")
        timestamp = mapdata.get('Timetamp')
        keys = list(mapdata.keys())

        # Extract the first 10 keys for Vdbs and the rest for Kbs
        Vdbs = {k: mapdata[k] for k in keys[:10]}
        Vdbs["Approved By"] = username + " at " + str(datetime.now())
        Kbs = {k: mapdata[k] for k in keys[10:]}

        # Query to find the existing exception record
        exception_record = db_session.query(ExceptionTable).filter_by(UserName=username, Address_ID=Address_ID, Timestamp=timestamp).first()

        if exception_record:
            map_creation_index = exception_record.MapCreation_Index

            # Delete the exception record
            db_session.delete(exception_record)
            db_session.commit()

            # Delete the linked MapCreation record, if it exists
            map_creation_record = db_session.query(MapCreationTable).filter_by(ID=map_creation_index).first()
            if map_creation_record:
                db_session.delete(map_creation_record)
                db_session.commit()

        # If the address is approved, add Kbs to the database
        if Vdbs.get("Address Approved?") == "Yes":
            CRUD.add_data(engine, Kbs)

            # Append the Vdbs data to Validation_DB.txt
            with open("Validation_DB.txt", 'r+') as file:
                try:
                    existing_data = json.load(file)
                except json.JSONDecodeError:
                    existing_data = []
                existing_data.append(Vdbs)
                file.seek(0)
                json.dump(existing_data, file, indent=4)
                file.truncate()
        else:
            # If rejected, handle the rejection case
            Vdbs["Rejected By"] = Vdbs.pop("Approved By")
            with open("ADDR_Rejection_DB.txt", "r+") as file:
                try:
                    existing_data = json.load(file)
                except json.JSONDecodeError:
                    existing_data = []
                existing_data.append(Vdbs)
                file.seek(0)
                json.dump(existing_data, file, indent=4)
                file.truncate()

        return jsonify({"status": "success", "message": "Form Data Received"})
    
    except Exception as e:
        app.logger.error(f"Error processing MapCreationForm: {e}")
        return jsonify({"status": "error", "message": "An error occurred while processing the form data"}), 500
    
    finally:
        db_session.close()




@app.route('/UserDefinedComponents', methods=["GET", "POST"])
def UD_Components():
    result = {}
    database_url = 'sqlite:///KnowledgeBase.db'
    Database_schema = CRUD(database_url)

    try:
        if request.method == "POST":
            # Fetch component data from the database
            component_data = Database_schema.get_Component_data()
            
            # Process and filter the component data
            for row in component_data:
                if row.component != "USAD_NA" and row.description != "Not Selected":
                    result[row.component] = row.description
            
            # Return the result as JSON
            return jsonify(result=result)

        elif request.method == "GET":
            # Return a message for the GET request
            return jsonify(message="GET request received for UserDefinedComponents")

    except Exception as e:
        app.logger.error(f"Error occurred in UD_Components: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


from sqlalchemy import func

@app.route("/add_new_component", methods=["POST"])
def add_new_component():
    result = {}
    session = Session()
    try:
        # Get the new component and description from the form data
        new_component = request.form.get('newComponent')
        new_description = request.form.get('newDescription')
        
        # Check if the component or description already exists (case-insensitive)
        existing_component = session.query(ComponentTable).filter(func.lower(ComponentTable.component) == func.lower(new_component)).first()
        existing_desc = session.query(ComponentTable).filter(func.lower(ComponentTable.description) == func.lower(new_description)).first()

        if existing_component or existing_desc:
            # Handle duplicate component or description
            if existing_component:
                result['error'] = 'Component already exists'
            if existing_desc:
                result['error'] = 'Component description already exists'
            return jsonify(result=result)

        # If no duplicates, add the new component to the database
        new_ud_component = ComponentTable(component=new_component, description=new_description)
        session.add(new_ud_component)
        session.commit()

        result['message'] = 'New component added successfully'
        return jsonify(result=result)

    except Exception as e:
        session.rollback()
        app.logger.error(f"Error occurred while adding new component: {e}")
        result['error'] = f'Error: {str(e)}'
        return jsonify(result=result), 500

    finally:
        session.close()  # Ensure session is always closed




@app.route('/save_changes', methods=['POST'])
def Edit_Components():
    result = {}
    session = Session()
    try:
        start_time = time.time()
        received_data = request.json['components']  # Get the combined old and modified data
        print("SaveChanges UDF Received data:", received_data)
        # Process and update Component Table using SQLAlchemy ORM


        for component_data in received_data:
            step_start = time.time()
            existing_component = session.query(ComponentTable).filter(func.lower(ComponentTable.component) == func.lower(component_data['newComponent'])).all()
            existing_desc = session.query(ComponentTable).filter(func.lower(ComponentTable.description) == func.lower(component_data['newDescription'])).all()
            existing_components = session.query(ComponentTable).filter(func.lower(ComponentTable.component))
            print("--------------------------------------")
            print(existing_components)
            print(f"\n\n\nExc Comp: {existing_component}\n\n\n")
            print(f"\n\n\nLength of Exc Desc: {len(existing_desc)}\n\n\n")
            if len(existing_component) >1 or len(existing_desc) >1:
                # Handle duplicate component or description
                if existing_component :
                    result['error'] = 'Component already exists'
                if existing_desc:
                    result['error'] = 'Component description already exists'
                return jsonify(result=result)
            # Identify the old component
            old_component = session.query(ComponentTable).filter_by(
                component=component_data['oldComponent'],
                description=component_data['oldDescription']
            ).first() 
            print(f"Query ComponentTable: {time.time() - step_start:.2f}s")
            step_start = time.time()

            if old_component:
                
                # Update the old component with new values
                old_component.component = component_data['newComponent']
                old_component.description = component_data['newDescription']

                # Perform bulk update for MappingJSON
                session.query(MappingJSON).filter_by(
                    component_index=component_data['oldComponent']
                ).update(
                    {"component_index": component_data['newComponent']},
                    synchronize_session=False  # Can be used for performance gain
                )
                print(f"Update MappingJSON: {time.time() - step_start:.2f}s")
                step_start = time.time()

                session.query(ExceptionTable).filter_by(
                    Component=component_data['oldComponent']
                ).update(
                    {"Component": component_data['newComponent']},
                    synchronize_session=False
                )

                print(f"Update ExceptionTable: {time.time() - step_start:.2f}s")

        # Commit all changes after processing
        session.commit()
        print(f"Total time taken: {time.time() - start_time:.2f}s")
        result['message'] = 'Component successfully Edited!'
        return jsonify(result=result)

    except Exception as e:
        session.rollback()  # Rollback the session in case of an error
        app.logger.error(f"Error occurred while adding new component: {e}")
        result['error'] = f'Error: {str(e)}'
        return jsonify(result=result), 500

    finally:
        session.close()  # Ensure the session is always closed


@app.route("/get_mask_count", methods=["GET"])
def get_mask_count():
    result = {}
    session = Session()
    
    try:
        # Get the component from the query parameters
        component = request.args.get('component')

        # Find the distinct mask indices associated with the component
        distinct_mask_indices = session.query(MappingJSON.mask_index)\
            .filter(MappingJSON.component_index == component)\
            .distinct()\
            .subquery()

        # Count the occurrences of these distinct mask indices in MaskTable
        total_masks = session.query(func.count(MaskTable.mask))\
            .filter(MaskTable.mask.in_(distinct_mask_indices))\
            .scalar()

        result['maskCount'] = total_masks

    except Exception as e:
        session.rollback()  # Rollback in case of error
        app.logger.error(f"Error occurred while getting mask count: {e}")
        return jsonify({'error': f'Error: {str(e)}'}), 500

    finally:
        session.close()  # Ensure the session is closed

    return jsonify(result=result)




@app.route("/delete_record", methods=["POST"])
def delete_component():
    if request.method == "POST":
        component = request.form.get('component')
        db_session = Session()
        dictionaries_deleted = {}

        try:
            # Step 1: Check if the component exists
            component_details = db_session.query(ComponentTable).filter_by(component=component).first()
            if not component_details:
                return jsonify(result={'message': 'Component not found'}), 404

            component_desc = component_details.description if component_details else "N/A"
            
            # Step 2: Get all mappings related to this component in a single query
            mappings_to_delete = db_session.query(MappingJSON).filter_by(component_index=component).all()

            if mappings_to_delete:
                # Step 3: Collect mask indices related to the mappings in one query
                mask_indices = [mapping.mask_index for mapping in mappings_to_delete]

                if mask_indices:
                    # Step 4: Delete related MaskTable entries in bulk
                    db_session.query(MaskTable).filter(MaskTable.mask.in_(mask_indices)).delete(synchronize_session=False)

                    # Step 5: Collect dictionaries for logging before deletion
                    mask_entries = db_session.query(MaskTable.mask).filter(MaskTable.mask.in_(mask_indices)).all()
                    for record in mask_entries:
                        result_dict = {}
                        data_records = db_session.query(MappingJSON).filter_by(mask_index=record.mask).all()
                        for data_record in data_records:
                            result_dict.setdefault(data_record.component_index, []).append(data_record.component_value)
                        dictionaries_deleted[record.mask] = result_dict

                # Step 6: Bulk delete mappings related to the component
                db_session.query(MappingJSON).filter_by(component_index=component).delete(synchronize_session=False)

            # Step 7: Delete the component itself
            db_session.delete(component_details)

            # Step 8: Commit all deletions in one go
            db_session.commit()

            return jsonify(result={'message': f'Record for component {component} deleted successfully'})

        except Exception as e:
            db_session.rollback()
            error_message = f"Error: {str(e)}"
            # print(error_message)
            return jsonify(result={'message': error_message}), 500

        finally:
            db_session.close()


@app.route('/authentication')
# @requires_role('Admin')
def authentication_page():
    session = Session()
    try:
        users = session.query(User).filter(User.Role != "Admin").all()
        # print("Fetched Users: ", users)
        # roles = session.query(UserRole).all()
        roles = session.query(UserRole).filter(UserRole.RoleName != "Admin").all()

        # print("Fetched Roles: ", roles)  # Debugging line


        user_data = [
            {
                'id': user.id, 
                'fullName': user.FullName, 
                'userName': user.UserName, 
                'email': user.Email, 
                'password' : "********",
                # 'Active' : user.Status,
                'role': user.role.RoleName  # Assuming a relationship attribute
                # 'status': 'Active' if user.isActive else 'Inactive'  # Assuming an isActive field
            } for user in users
        ]
        role_data = [role.RoleName for role in roles]
        # print("role_data : ", role_data)
        # print("user_data : ", user_data)
        return jsonify({'users': user_data, 'roles': role_data})
    except Exception as e:
        # print("Error: ", e)
        return jsonify({'users': [], 'roles': []})
    finally:
        session.close()

# @app.route('/CRUDUser', methods=["GET", "POST"])
# @requires_role('Admin')
# def CRUDUser():
#     sessions = Session() 
#     users = sessions.query(User).all()
#     sessions.close()
#     # print(users)
#     return render_template('index.html', users=users)


@app.route('/save_User/<int:user_id>', methods=['POST'])
def edit_user(user_id):
    session = Session()
    # print("User ID", user_id)
    try:
        user = session.query(User).get(user_id)  # Find the user by ID
        # print("Users: ", user)
        UserDetails = request.get_json()
        # print("\n\nUserDetails: ", UserDetails, "\n\n")

        if user:
            # Update user details
            user.FullName = UserDetails.get('FullName')
            user.UserName = UserDetails.get('UserName')
            user.Email = UserDetails.get('Email')
            user.Role = UserDetails.get('Role_id')
            # user.Status = UserDetails.get('Status')

            session.commit()

        return jsonify({"message": "User updated successfully"}), 200
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/create_user', methods=['POST'])
def create_user():
    session = Session()
    try:
        UserDetails = request.get_json()
        # print("create_user Details: ", UserDetails)
        user = session.query(User).get(UserDetails["UserName"])  # Find the user by ID
        # print("user: ",user)
        # Create a new user instance
        new_user = User()
        new_user.FullName = UserDetails.get('FullName')
        new_user.UserName = UserDetails.get('UserName')
        new_user.Email = UserDetails.get('Email')
        
        hashed_password = hash_password(UserDetails.get('Password'))
        new_user.Password = hashed_password

        new_user.Role = UserDetails.get('Role_id')
        # new_user.Status = UserDetails.get('Status')
        # print("New User Ready to Add: ",new_user)
        # Add the new user to the session and commit
        session.add(new_user)
        session.commit()

        return jsonify({"message": "User updated successfully"}), 200
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/delete_User/<int:user_id>', methods=['POST'])
def delete_user(user_id):
    session = Session()
    try:
        user = session.query(User).get(user_id)  # Find the user by ID
        if user:
            session.delete(user)  # Delete the user
            session.commit()  # Commit the changes

        return redirect(url_for('/authentication'))
    except Exception as e:
        session.rollback()
        return str(e)
    finally:
        session.close()

# -------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------ClueTable-------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

from ORM import ClueTable
@app.route('/ClueComponents', methods=["GET", "POST"])
def CLUE_Components():
    session = Session()
    result = {}

    try:
        if request.method == "POST":
            data = request.get_json()

            if not data:
                return jsonify(result={'error': 'No data provided'}), 400

            mask_token = data.get('maskToken')
            query = data.get('query', '').lower()

            # Build the query
            query_filter = session.query(ClueTable)
            if mask_token:
                query_filter = query_filter.filter(func.lower(ClueTable.token) == mask_token.lower())

            clue_data = query_filter.all()

            # Filter by the query string within the component description
            for row in clue_data:
                component = row.component_desc
                token = row.token

                if query in component.lower():
                    result[component] = token

            return jsonify(result=result)

        return jsonify(result={})

    except Exception as e:
        print("\n\n\nRollBack for Clue Table triggered!!!\n\n\n")
        session.rollback()  # Rollback in case of error
        app.logger.error(f"Error occurred in CLUE_Components: {e}")
        return jsonify({'error': f'Error: {str(e)}'}), 500

    finally:
        session.close()  # Ensure the session is closed


from ORM import ClueTable
@app.route('/get_token', methods=['GET'])
def get_token():
    session = Session()
    try:
        # Query for distinct tokens from ClueTable
        descriptions = session.query(ClueTable.token).distinct().all()
        
        # Extract tokens and sort them
        descriptions_list = [desc.token for desc in descriptions]
        descriptions_list.sort()

        return jsonify(descriptions_list)

    except Exception as e:
        session.rollback()  # Rollback in case of an error
        app.logger.error(f"Error occurred while fetching tokens: {e}")
        return jsonify({'error': f'Error: {str(e)}'}), 500

    finally:
        session.close()  # Ensure the session is closed



@app.route('/update_clue', methods=['POST'])
def update_clue():
    data = request.json
    old_component = data['oldComponent']
    old_description = data['oldDescription']
    new_component = data['newComponent']
    new_description = data['newDescription']

    session = Session()
    try:
        # Check if the new component already exists, excluding the current row
        existing_clue = session.query(ClueTable).filter(ClueTable.component_desc == new_component).first()

        if existing_clue and (existing_clue.component_desc != old_component or existing_clue.token != old_description):
            return jsonify({'status': 'duplicate', 'message': 'Token already exists'}), 400

        # Fetch the existing clue to update
        clue = session.query(ClueTable).filter_by(component_desc=old_component, token=old_description).first()
        if clue:
            # Update the clue
            clue.component_desc = new_component
            clue.token = new_description
            session.commit()
            return jsonify({'status': 'success'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Clue not found'}), 404
    except Exception as e:
        session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        session.close()





@app.route("/add_new_ClueComponent", methods=["POST"])
def add_new_ClueComponent():
    result = {}
    session = Session()

    try:
        data = request.get_json()
        new_mask = data.get('newMask')  # Assuming this is the component description
        new_token = data.get('newToken')
        print(new_mask)
        # Check if the token already exists in the clue table
        existing_clue = session.query(ClueTable).filter_by(component_desc=new_token).first()
        print(existing_clue)
        if existing_clue:
            result['error'] = 'Token already exists in the clue table.'
        else:
            # Add the new Clue component
            new_Clue_component = ClueTable(component_desc=new_mask, token=new_token)
            session.add(new_Clue_component)
            session.commit()
            result['message'] = 'New component added successfully'

    except Exception as e:
        session.rollback()  # Rollback in case of any error
        app.logger.error(f"Error adding new clue component: {e}")
        result['error'] = f"An error occurred: {str(e)}"

    finally:
        session.close()  # Ensure session is closed

    return jsonify(result)





from flask import request, jsonify
import sqlite3

@app.route('/deleteClue', methods=['POST'])
def delete_clue():
    session = Session()  # Use SQLAlchemy session for database queries
    try:
        data = request.get_json()
        component_desc = data.get('component_desc')
        
        # Query the ClueTable to find the entry
        clue_to_delete = session.query(ClueTable).filter_by(component_desc=component_desc).first()
        
        if clue_to_delete:
            # Delete the entry from the database
            session.delete(clue_to_delete)
            session.commit()
            return jsonify({'success': True}), 200
        else:
            return jsonify({'success': False, 'error': 'Clue not found'}), 404
    
    except Exception as e:
        session.rollback()  # Rollback in case of error
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()






    
if __name__ == '__main__':
    
    app.run(port=8080, debug=True)

