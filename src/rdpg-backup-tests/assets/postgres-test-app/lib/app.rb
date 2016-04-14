require 'sinatra'
require 'pg'
require 'cf-app-utils'
require 'json'

DATA ||= {}
SUCCESS_MESSAGE = "SUCCESS"
FAILURE_MESSAGE = "FAILURE"

def postgres_uri
  return nil unless ENV['VCAP_SERVICES']

  JSON.parse(ENV['VCAP_SERVICES'], :symbolize_names => true).values.map do |services|
    services.each do |s|
      #check if this is actually a SQL service. Ugly, but should work.
      if s.has_key?(:credentials)
        c = s[:credentials]
        if c.has_key?(:uri) and c.has_key?(:jdbc_uri)
          return s[:credentials][:uri]
        end
      end
    end
  end
  nil
end

# entries table (an entry not present in the uri will be nil)
# :username string
# :password string
# :location string
# :port     string
# :dbname   string
# :params   string
def uri_entries
	#uri format: postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
	uri = postgres_uri
	unless uri
		return nil
	end
	if uri.index('postgresql://') == 0
		uri = uri['postgresql://'.length...uri.length]
	elsif uri.index('postgres://') == 0
		uri = uri['postgres://'.length...uri.length]
	else
		return nil #if it doesn't start with that string, the uri is invalid.
	end
	entries = {}
	login_cred_end = uri.index('@')
	#if there's '@', then we need to parse login credentials
	if login_cred_end
		login_creds = uri[0...login_cred_end]
		uri = uri[(login_cred_end + 1)...uri.length] #makes later code cleaner.
		username_end = login_creds.index(':')
		if username_end
			entries[:username] = login_creds[0...username_end]
			entries[:password] = login_creds[(username_end + 1)...login_creds.length]
		else
			entries[:username] = login_creds
		end
	end
	params_start = uri.index('?')
	if params_start
		entries[:params] = uri[(params_start + 1)...uri.length]
		uri = uri[0...params_start]
	end
	dbname_start = uri.index('/')
	if dbname_start
		entries[:dbname] = uri[(dbname_start + 1)...uri.length]
		uri = uri[0...dbname_start]
	end
	port_start = uri.index(':')
	if port_start
		entries[:port] = uri[(port_start + 1)...uri.length]
		uri = uri[0...port_start]
	end
	#what's left should be the url location
	unless uri.empty?
		entries[:location] = uri
	end
	return entries
end

#gets a fresh connection to the database
def get_conn
  begin
    return PG::Connection.new(postgres_uri)
  rescue PG::Error
    return nil
  end
end

#This should display the timestamp if a connection to the database was established.
#The regex matches '/' or '/timestamp' or '/timestamp/'
get /\A\/(timestamp(\/)?)?\z/ do
  conn = get_conn
  if conn == nil
    body FAILURE_MESSAGE
    status 409
  else 
    begin
      res = conn.exec("SELECT CURRENT_TIMESTAMP;")
      status 200
      output = "#{SUCCESS_MESSAGE}\n#{res.getvalue(0,0)}"
      body output
    rescue PG::Error
      status 409
      body FAILURE_MESSAGE
		end
    conn.close()
  end
end

#just an endpoint that should respond if this app is running.
get '/ping/?' do
  status 200
  body SUCCESS_MESSAGE
end

#displays the uri to access the postgres database
get /\A\/uri\/?\z/ do
  res = postgres_uri
  if res
    body "#{SUCCESS_MESSAGE}\n#{res}"
    status 200
  else
    body FAILURE_MESSAGE
    status 409
  end
end

# Most postgres service listings will probably have dbname, user, pass, etc
# listed separately. However, I don't want that to be a requirement to use this
# app, so I'll pull the necessary info direct from the uri.

get '/uri/username/?' do
	entries = uri_entries
	if entries and entries[:username]
		body "#{SUCCESS_MESSAGE}\n#{entries[:username]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/uri/password/?' do
	entries = uri_entries
	if entries and entries[:password]
		body "#{SUCCESS_MESSAGE}\n#{entries[:password]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/uri/location/?' do
	entries = uri_entries
	if entries and entries[:location]
		body "#{SUCCESS_MESSAGE}\n#{entries[:location]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/uri/port/?' do
	entries = uri_entries
	if entries and entries[:port]
		body "#{SUCCESS_MESSAGE}\n#{entries[:port]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/uri/dbname/?' do
	entries = uri_entries
	if entries and entries[:dbname]
		body "#{SUCCESS_MESSAGE}\n#{entries[:dbname]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/uri/params/?' do
	entries = uri_entries
	if entries and entries[:params]
		body "#{SUCCESS_MESSAGE}\n#{entries[:params]}"
	else
		body FAILURE_MESSAGE
		status 409
	end
end

get '/services/?' do
  res = ENV['VCAP_SERVICES']
  if res
    body "#{SUCCESS_MESSAGE}\n#{res}"
    status 200
  else
    body FAILURE_MESSAGE
    status 409
  end
end

#execute an arbitrary, user-supplied query on the database
post '/exec/?' do
  conn = get_conn
  begin
    unless params['sql']
      halt 500, 'NO-SQL-QUERY'
    end
    res = conn.exec(params['sql'])
    if res.num_tuples > 0
      output = "#{SUCCESS_MESSAGE}\n"
      output = output << JSON.generate(res.values)
      status 200
      body output
    else
      body SUCCESS_MESSAGE
      status 200
    end
  rescue PG::Error
    body FAILURE_MESSAGE
    status 409
  end
  conn.close()
end

error do
  halt 500, "ERR:#{env['sinatra.error']}"
end

