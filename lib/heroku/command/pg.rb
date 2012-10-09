require 'thread'

require "heroku/command/base"

module Heroku::Sudo::Client

module Heroku
  # Graft the lockout methods onto the API class
  class API
    def put_lockout_halt(app)
      request(
              :expects  => 200,
              :method   => :put,
              :path     => "/apps/#{app}/activity",
              :query    => { 'state' => 'halt' }
              )
    end

    def put_lockout_resume(app)
      request(
              :expects  => 200,
              :method   => :put,
              :path     => "/apps/#{app}/activity",
              :query    => { 'state' => 'halt' }
              )
    end
end

class Heroku::Command::Pg < Heroku::Command::Base

  include Heroku::Helpers

  # pg:migrate SHEN_URL
  #
  # Migrate from legacy shared databases to Heroku Postgres Dev
  #
  #     --remove-shared-addon plan # Remove the shared database addon when done
  def migrate
    shen_url = shift_argument
    validate_arguments!

    if shen_url == nil
      display("No Shen URL passed to pg:migrate")
      exit(23)
    end

    maintenance = Heroku::PgMigrate::Maintenance.new(api, app)
    lockout = Heroku::PgMigrate::Lockout.new(api, app)
    rebind = Heroku::PgMigrate::RebindConfig.new(api, app, shen_url)
    provision = Heroku::PgMigrate::Provision.new(api, app)
    foi_pgbackups = Heroku::PgMigrate::FindOrInstallPgBackups.new(api, app)
    transfer = Heroku::PgMigrate::Transfer.new(api, app, shen_url)

    mp = Heroku::PgMigrate::MultiPhase.new()
    mp.enqueue(foi_pgbackups)
    mp.enqueue(provision)
    mp.enqueue(maintenance)
    mp.enqueue(lockout)
    mp.enqueue(transfer)
    mp.enqueue(rebind)

    # Conditionally enqueue the removal of the shared database plan
    if options[:remove_shared_addon]
      unvalidated = options[:remove_shared_addon]
      okay_plans = ["shared-database:20gb", "shared-database:5mb"]

      if okay_plans.member? unvalidated
        # Okay, this is *now* a validated plan name to remove
        mp.enqueue(Heroku::PgMigrate::RemoveShared.new(api, app, unvalidated))
      else
        # This is not a valid plan name
        display("Was passed this plan to remove: #{unvalidated}, but " +
          "expected one of #{okay_plans}")
        exit(23)
      end
    end

    begin
      mp.engage()

      if mp.success
        # Rollbacks are more like "finally" when there is no exception
        # thrown.
        mp.class.process_rollbacks(mp.rollbacks, nil)
        display('Migration completed successfully.')
        exit(0)
      else
        # Really bad news, because a failure without an exception is
        # not intended to ever occur.  This more akin to an assertion
        # failure.
        display("FATAL: not successful, but no exception.  Check carefully.")
      end

      # Paranoia to be robust against code change
      exit(83)
    rescue SystemExit
      # Do Nothing
      raise
    rescue Exception, Interrupt => e
      display('Process rollbacks')
      display(mp.caught_exception.inspect)
      mp.class.process_rollbacks(mp.rollbacks, mp.caught_exception)
      display('Rollbacks processed')

      display (<<EOF
 !
 !    ROLLBACK BACKTRACE
 !

EOF
)
      display(e.backtrace)
      display("Rolled back because of exception:\n#{e.inspect}")
      display("Migration failed")
      exit(97)
      raise
    end
  end

  def prepostinvariant
    # This program displays strings that should be the same before and
    # after migration.
    display('Maintenance mode: ' +
      Heroku::PgMigrate::Maintenance.fetch_maintenance_status(api, app).to_s)
    transient_processes = [ 'run', 'scheduler' ]
    display('process-counts' + ":\t" +
      Heroku::PgMigrate::Lockout.process_count(api, app).reject { |key, value|
              transient_processes.include? key }.sort.inspect)
  end

  def releasereport
    display('Maintenance mode: ' +
      Heroku::PgMigrate::Maintenance.fetch_maintenance_status(api, app).to_s)

    crel = api.get_release(app, 'current')
    ['name', 'pstable', 'addons', 'env'].each { |n|
      display(n + ":\t" + crel.body.fetch(n).inspect)
    }

    display('process-counts' + ":\t" +
      Heroku::PgMigrate::Lockout.process_count(api, app).inspect)
  end
end

module Heroku::PgMigrate
end

module Heroku::PgMigrate::NeedRollback
end

module Heroku::PgMigrate
  XactEmit = Struct.new(:more_actions, :more_rollbacks, :feed_forward)
end

class Heroku::PgMigrate::CannotMigrate < RuntimeError
end

class Heroku::PgMigrate::MultiPhase
  include Heroku::Helpers

  attr_reader :caught_exception, :rollbacks, :success

  def initialize()
    @to_perform = Queue.new
    @rollbacks = []
    @success = false
    @caught_exception = nil
  end

  def enqueue(xact)
    @to_perform.enq(xact)
  end

  def engage
    feed_forward = {}

    loop do
      break if @to_perform.length == 0
      xact = @to_perform.deq()

      # Finished all xacts without a problem
      break if xact.nil?

      emit = nil

      begin
        begin
          emit = xact.perform!(feed_forward)
        rescue Exception, Interrupt => error
          # Always save the exception raised for rollback behavior
          @caught_exception = error
          raise
        end
      rescue Heroku::PgMigrate::NeedRollback => error
        @rollbacks.push(xact)
        raise
      rescue Heroku::PgMigrate::CannotMigrate => error
        # Just need to print the message, run the rollbacks --
        # guarded by "ensure" -- and exit cleanly.
        hputs(error.message)
        raise
      end

      # Many actions do not need to set up any rollbacks and do not
      # have any state to feed forward, so ease the burden on those by
      # allowing them to return nil to mean "do nothing".
      if emit != nil
        emit.more_actions.each { |nx|
          @to_perform.enq(nx)
        }

        @rollbacks.concat(emit.more_rollbacks)
        if emit.feed_forward != nil
          # Use the class value as a way to coordinate between
          # different steps.
          #
          # In principle, though, the class is not required per se,
          # one only neesd a value that multiple passes can know about
          # before beginning execution yet is known to be unique among
          # all actions in execution.
          feed_forward[xact.class] = emit.feed_forward
        end
      end
    end

    @success = true
    return feed_forward
  end

  def self.process_rollbacks(rollbacks, caught_exception)
    # Rollbacks are intended to be idempotent (as they may get run
    # one or more times unless someone completely kills the program)
    # and we'd *really* prefer them run until they are successful,
    # no matter what.
    #
    # NB: this code is not completely sound in handling all interrupts
    # at any time, and to be so it would have to learn to be
    # re-entrant: consider if there is an interrupt when entering this
    # procedure, but before any work gets done.  However, in event of
    # such an error a Fatal error is registered by shen-migrator via
    # the prepostinvariant procedure.
    #
    # So instead, just treat the common case as to avoid vanilla API
    # errors from causing too much harm, and exit if somethihng causes
    # the rollback to get incredibly stuck.
    loop do
      xact = rollbacks.pop()
      break if xact.nil?

      attempts = 0
      loop do
        if attempts > 10
          exit(26)
        end

        begin
          # Some actions have no sensible rollback, but this conditional
          # allows them to avoid writing noop rollback! methods all the
          # time.
          if xact.respond_to?(:rollback!)
            xact.rollback!(caught_exception)
          end

          break
        rescue Exception, Interrupt => e
          puts e.to_s
          attempts += 1
        end
      end
    end
  end
end

class Heroku::PgMigrate::Maintenance
  include Heroku::Helpers

  def initialize(api, app)
    @api = api
    @app = app
    @precondition_mm = nil
  end

  def perform!(ff)
    # Avoid setting/un-setting maintenance mode if it was on to begin
    # with.
    action("Checking if maintenance mode already set") {
      @precondition_mm = self.class.fetch_maintenance_status(@api, @app)
      status("is #{@precondition_mm}")
    }

    begin
      if !@precondition_mm
        action("Entering maintenance mode on application #{@app}") {
          @api.post_app_maintenance(@app, '1')
        }
      else
        display("Maintenance mode was already set on #{@app}, doing nothing")
      end

      # Always want to rollback, regardless of exceptions or their
      # absence.  However, exceptions must be propagated as to
      # interrupt continued execution.
      return Heroku::PgMigrate::XactEmit.new([], [self], nil)
    rescue Exception => error
      error.extend(Heroku::PgMigrate::NeedRollback)
      raise
    end
  end

  def rollback!(reason)
    if @precondition_mm
      display("No need to leave maintenance mode, because:\n" +
        "\tprecondition: #{@precondition_mm}\n")
    else
      action("Leaving maintenance mode for application #{@app}") {
        @api.post_app_maintenance(@app, '0')
      }
    end
  end

  #
  # Helper Procedures
  #
  def self.fetch_maintenance_status(api, app)
    mm = api.get_app_maintenance(app).body.fetch('maintenance')

    if mm != false && mm != true
      raise Heroku::PgMigrate::CannotMigrate.new(
        'ERROR: Maintenance mode violates two-valued logic: is ' +
        mm.inspect)
    end

    return mm
  end
end

class Heroku::PgMigrate::Lockout
  include Heroku::Helpers

  def initialize api, app
    @api = api
    @app = app
  end

  def perform!(ff)
    # Perform the actual lockout
    lockout!

    # Always want to rollback, regardless of exceptions or their
    # absence.  However, exceptions must be propagated as to
    # interrupt continued execution.
    return Heroku::PgMigrate::XactEmit.new([], [self], nil)
  rescue Exception => error
    error.extend(Heroku::PgMigrate::NeedRollback)
    raise
  end

  def rollback!(reason)
    action("Removing application lockout") {
      @api.put_lockout_resume(@app)
    }
  end

  #
  # Helper procedures
  #

  def self.process_count(api, app)
    # Read an app's process names and compute their quantity.

    processes = api.get_ps(app).body

    old_counts = {}
    processes.each do |process|
      name = process["process"].split(".").first

      # Is there a better way to ask the API for how many of each
      # process type is the target to be run?  Right now, compute it
      # by parsing the textual lines.
      if old_counts[name] == nil
        old_counts[name] = 1
      else
        old_counts[name] += 1
      end
    end

    return old_counts
  end

  def lockout!
    action("Locking out application process scaling") {
      @api.put_lockout_halt(@app)
    }
  end
end

class Heroku::PgMigrate::RebindConfig
  include Heroku::Helpers

  def initialize(api, app, shen_url)
    @api = api
    @app = app
    @old = shen_url
    @rebinding = nil
  end

  def perform!(ff)
    # Unpack information from provisioning step
    pdata = ff.fetch(Heroku::PgMigrate::Provision)
    config = pdata.config_var_snapshot
    new_url = config.fetch(pdata.env_var_name)

    # Compute all the configuration variables that need rebinding.
    rebinding = self.class.find_rebindings(config, @old)

    # Indicate what is about to be done
    action("Binding new database configuration to: " +
      "#{self.class.humanize(rebinding)}") {

      # Set up state for rollback
      @rebinding = rebinding

      begin
        self.class.rebind(@api, @app, rebinding, new_url)
      rescue Exception => error
        # If this fails, rollback is necessary
        error.extend(Heroku::PgMigrate::NeedRollback)
        raise
      end
    }

    return nil
  end

  def rollback!(reason)
    if @rebinding.nil? || @old.nil?
      # Apparently, perform! never got far enough to bind enough
      # rollback state.
      raise "Internal error: rollback performed even though " +
        "this action should not require undoing."
    end

    action("Binding old database configuration to: " +
      "#{self.class.humanize(@rebinding)}") {
      self.class.rebind(@api, @app, @rebinding, @old)
    }
  end


  #
  # Helper procedures
  #

  def self.find_rebindings(vars, old)
    # Yield each configuration variable with a given value.
    rebinding = []
    vars.each { |name, val|
      if val == old
        rebinding << name
      end
    }

    return rebinding
  end

  def self.rebind(api, app, names, val)
    # Rebind every configuration in 'names' to 'val'
    exploded_bindings = {}
    names.each { |name|
      exploded_bindings[name] = val
    }

    api.put_config_vars(app, exploded_bindings)
  end

  def self.humanize(names)
    # How a list of rebound configuration names are to be rendered
    names.join(', ')
  end
end

class Heroku::PgMigrate::Provision
  include Heroku::Helpers

  ForwardData = Struct.new(:env_var_name, :config_var_snapshot)

  def initialize(api, app)
    @api = api
    @app = app
    @addon_name = nil
  end

  def perform!(ff)
    config_var_name = nil
    config_vars = nil

    action("Installing heroku-postgresql:dev") {
      addon = @api.post_addon(@app, 'heroku-postgresql:dev')

      # Parse out the bound variable name
      add_msg = addon.body["message"]
      add_msg =~ /^Attached as (HEROKU_POSTGRESQL_[A-Z]+)(?:_URL)?$/
      @addon_name = $1
      status("attached as #{@addon_name}")

      config_var_name = @addon_name + '_URL'
      config_vars = @api.get_config_vars(@app).body
    }

    return Heroku::PgMigrate::XactEmit.new([], [self],
      ForwardData.new(config_var_name, config_vars))
  rescue Exception => error
    error.extend(Heroku::PgMigrate::NeedRollback)
    raise
  end

  def rollback!(reason)
    if reason != nil
      if @addon_name.nil?
        display("Couldn't delete addon after failed migration:\n" +
          "did not receive its name (it may still have been added)")
      else
        display("Deleting addon after failed migration: #{@addon_name}")
        @api.delete_addon(@app, @addon_name)
      end
    end
  end
end

class Heroku::PgMigrate::FindOrInstallPgBackups
  include Heroku::Helpers

  def initialize(api, app)
    @api = api
    @app = app
  end

  def perform!(ff)
    added = nil

    action("Checking for pgbackups addon") {
      begin
        addon = @api.post_addon(@app, 'pgbackups:plus')
        status("not found")
        added = true
      rescue Heroku::API::Errors::RequestFailed => error
        added = false
        err_msg = error.response.body["error"]

        if err_msg == "Add-on already installed" ||
            (err_msg =~ /pgbackups:[a-z]+ add-on already added\./).nil?
          status("already present")
        else
          raise
        end
      end
    }

    if added
      # Actually already happened, since the addition/detection is
      # done in one step, but write out some activity to show that
      # this did happen.
      action("Adding pgbackups") {
        # no-op, but block must be passed
      }
    end

    return nil
  end
end

class Heroku::PgMigrate::Transfer
  include Heroku::Helpers

  def initialize(api, app, shen_url)
    @api = api
    @app = app
    @shen_url = shen_url
  end

  def perform!(ff)
    pdata = ff.fetch(Heroku::PgMigrate::Provision)
    config = pdata.config_var_snapshot
    pgbackups_url = config.fetch('PGBACKUPS_URL')
    from_url = @shen_url
    to_url = config.fetch(pdata.env_var_name)

    pgbackups_client = Heroku::Client::Pgbackups.new(pgbackups_url)
    renderer = Renderer.new

    action("Transferring") {
      backup = pgbackups_client.create_transfer(
        from_url, "legacy-db", to_url, 'hpg-shared')
      backup = renderer.poll_transfer!(pgbackups_client, backup)
      if backup["error_at"]
        puts backup.inspect
        raise Heroku::PgMigrate::CannotMigrate.new(
          "ERROR: Transfer failed, aborting.")
      end
    }

    return nil
  end

  class Renderer
    # A copy from heroku/command/pgbackups.rb for backup progress
    # rendering, so it can be easily tweaked.
    include Heroku::Helpers
    include Heroku::Helpers::HerokuPostgresql


    def poll_error(app)
      error <<-EOM
Failed to query the PGBackups status API. Your backup may still be running.
Verify the status of your backup with `heroku pgbackups -a #{app}`
      EOM
    end


    def poll_transfer!(pgbackups_client, transfer)
      display "\n"

      if transfer["errors"]
        transfer["errors"].values.flatten.each { |e|
          output_with_bang "#{e}"
        }
        puts transfer.inspect
        raise Heroku::PgMigrate::CannotMigrate.new(
          "ERROR: Transfer failed, aborting.")
      end

      while true
        update_display(transfer)
        break if transfer["finished_at"]

        sleep_time = 1
        begin
          sleep(sleep_time)
          transfer = pgbackups_client.get_transfer(transfer["id"])
        rescue RestClient::ServiceUnavailable
          if sleep_time > 300
            poll_error(@app)
          else
            sleep_time *= 2
            display "Retrying after #{sleep_time} seconds\n"
            retry
          end
        rescue Exception => e
          puts "Aborting due to other error in transfer:"
          puts e.inspect
          puts e.backtrace.inspect
          raise
        end
      end

      display "\n"

      return transfer
    end

    def update_display(transfer)
      @ticks            ||= 0
      @last_updated_at  ||= 0
      @last_logs        ||= []
      @last_progress    ||= ["", 0]

      @ticks += 1

      step_map = {
        "dump"      => "Capturing",
        "upload"    => "Storing",
        "download"  => "Retrieving",
        "restore"   => "Restoring",
        "gunzip"    => "Uncompressing",
        "load"      => "Restoring",
      }

      if !transfer["log"]
        @last_progress = ['pending', nil]
        redisplay "Pending... #{spinner(@ticks)}"
      else
        logs        = transfer["log"].split("\n")
        new_logs    = logs - @last_logs
        @last_logs  = logs

        new_logs.each do |line|
          matches = line.scan /^([a-z_]+)_progress:\s+([^ ]+)/
          next if matches.empty?

          step, amount = matches[0]

          if ['done', 'error'].include? amount
            # step is done, explicitly print result and newline
            redisplay "#{@last_progress[0].capitalize}... #{amount}\n"
          end

          # store progress, last one in the logs will get displayed
          step = step_map[step] || step
          @last_progress = [step, amount]
        end

        step, amount = @last_progress
        unless ['done', 'error'].include? amount
          redisplay "#{step.capitalize}... #{amount} #{spinner(@ticks)}"
        end
      end
    end
  end
end

class Heroku::PgMigrate::RemoveShared
  include Heroku::Helpers

  ForwardData = Struct.new(:env_var_name, :config_var_snapshot)

  def initialize(api, app, plan_name)
    @api = api
    @app = app
    @plan_name = plan_name
  end

  def perform!(ff)
    display("Will delete original #{@plan_name} at the end of the migration")
    return Heroku::PgMigrate::XactEmit.new([], [self], nil)
  end

  def rollback!(reason)
    if reason.nil?
      display("Deleted shared database addon: #{@plan_name}")
      @api.delete_addon(@app, @plan_name)
    else
      display("keeping shared database addon because of incomplete migration")
    end
  end
end
