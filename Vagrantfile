unless Vagrant.has_plugin?("vagrant-reload")
  raise 'Install vagrant-reload "vagrant plugin install vagrant-reload"'
end

Vagrant.configure("2") do |config|

  config.vm.box = "archlinux/archlinux"
  config.vm.hostname = "lsdp-l6"

  config.vm.network :private_network, ip: "10.0.0.2"
  config.vm.network "forwarded_port", guest: 8080, host: 8080
  config.vm.network "forwarded_port", guest: 8081, host: 8081
  config.vm.network "forwarded_port", guest: 4040, host: 4040
  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end
  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder "project/", "/project", mount_options: ["dmode=777,fmode=777"] #who cares
  config.vm.synced_folder "project/example", "/home/vagrant/go/src/example", mount_options: ["dmode=777,fmode=777"] #who cares
  

  config.vm.provision :ansible do |ansible|
    ansible.playbook = "ansible/playbook.yml"
    ansible.verbose = true
  end


  # Shell scripts for Vagrant provisioning if you have problems with Ansible
  # config.vm.provision "update", type: "shell", path: "shell/update.sh"
  # config.vm.provision :reload

end
