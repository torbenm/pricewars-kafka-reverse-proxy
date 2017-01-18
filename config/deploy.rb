# config valid only for current version of Capistrano
# lock '3.4.0'

set :application, "pricewars-kafka-reverse-proxy"
set :repo_url, "git@github.com:hpi-epic/pricewars-kafka-reverse-proxy.git"
set :scm, :git

set :pty, true

set :format, :pretty

# Default value for keep_releases is 5
set :keep_releases, 5

# Default branch is :master
# ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

set :default_env, rvm_bin_path: "~/.rvm/bin/"
# set :bundle_gemfile, "backend/Gemfile"
# set :repo_tree, 'backend'

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/var/www/pricewars-kafka-rest"

# Default value for :scm is :git
# set :scm, :git
set :rvm_ruby_version, "2.3.1"

# Default value for :format is :pretty
# set :format, :pretty

# Default value for :log_level is :debug
# set :log_level, :debug

# Default value for :pty is false
# set :pty, true

# Default value for :linked_files is []
# set :linked_files, fetch(:linked_files, []).push('config/database.yml', 'config/secrets.yml')

# Default value for linked_dirs is []
# set :linked_dirs, fetch(:linked_dirs, []).push('log', 'tmp/pids', 'tmp/cache', 'tmp/sockets', 'vendor/bundle', 'public/system')

# Default value for default_env is {}
# set :default_env, { path: "/opt/ruby/bin:$PATH" }

# Default value for keep_releases is 5
# set :keep_releases, 5

namespace :deploy do
  task :install_dependencies do
    on roles :all do
      within release_path do
        execute "cd #{release_path}/ && sudo pip3 install -r requirements.txt"
        execute "cd #{release_path}/ && sudo pip3 install mod_wsgi"
        execute "cd #{release_path}/ && sudo pip3 install gunicorn"
      end
    end
  end
  task :restart_webserver do
    on roles :all do
      within release_path do
        #execute "pkill -f gunicorn" # return 1 per default..
        #execute "ps -ef | grep gunicorn | grep -v grep | awk '{print $2}' | sudo xargs kill -9"
        #execute "cd #{release_path}/ && gunicorn --worker-class eventlet -w 1 -b 0.0.0.0:8001 --reload LoggerApp:app >> gunicorn.log &"
        execute "sudo service loggerapp restart"
      end
    end
  end

  after :deploy, "deploy:install_dependencies"
  after :deploy, "deploy:restart_webserver"
end
