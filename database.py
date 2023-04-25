from sqlalchemy import create_engine, text
import os

db_connection_string = os.environ['ABHI_CONNECTION_KEY']

engine = create_engine(db_connection_string,
                       connect_args={"ssl": {
                         "ssl_ca": "/etc/ssl/cert.pem"
                       }})


def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(
      text(
        "SELECT id, title, location, salary, currency, responsibilities, requirements FROM jobs"
      ))
    jobs = []
    for row in result.fetchall():
      job = {
        'id': row[0],
        'title': row[1],
        'location': row[2],
        'salary': row[3],
        'currency': row[4],
        'responsibilities': row[5],
        'requirements': row[6]
      }
      jobs.append(job)
    return jobs


def load_job_from_db(id):
  with engine.connect() as conn:
    result = conn.execute(text(f"SELECT * FROM jobs WHERE id = {id}"), )
    rows = result.all()
    if len(rows) == 0:
      return None
    else:
      return rows[0]._mapping


def add_application_to_db(job_id, data):
  with engine.connect() as conn:
    query = text(
      """INSERT INTO applications (job_id, full_name, email, linkedin_url, education, work_experience, resume_url)
                        VALUES (:job_id, :full_name, :email, :linkedin_url, :education, :work_experience, :resume_url)"""
    )
    params = {
      'job_id': job_id,
      'full_name': data['full_name'],
      'email': data['email'],
      'linkedin_url': data['linkedin'],
      'education': data['education'],
      'work_experience': data['workexp'],
      'resume_url': data['resumelink']
    }
    conn.execute(query, params)


def add_job_to_db(data):
  with engine.connect() as conn:
    query = text("""INSERT INTO jobs ( id,title, location, salary, 
      currency, responsibilities, requirements) 
      VALUES(:id ,:title, :location, :salary, :currency, :responsibilities, 
      :requirements)""")
    params = {
      'id': data['id'],
      'title': data['title'],
      'location': data['location'],
      'salary': data['salary'],
      'currency': data['currency'],
      'responsibilities': data['responsibilities'],
      'requirements': data['requirements']
    }
    conn.execute(query, params)
