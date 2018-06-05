provider "aws" {
   region = "us-west-2"
}

resource "aws_key_pair" "mabuaita-ssh" {
   key_name = "mabuaita-ssh"
   public_key = "${file("/Users/mabuaita/.ssh/id_rsa.pub")}"
}
 
resource "aws_instance" "base-vm" {
   ami = "ami-4e79ed36"
   instance_type = "t2.meduim"
 
   tags {
     Name = "env-infra-vpn"
   }

  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size = 16
    }
  }

  credit_specification {
    cpu_credits = "unlimited"
  }

  network_interfaces {
    associate_public_ip_address = true
  }
  
  subnet_id = subnet-cc79748a

  placement {
    availability_zone = "us-west-2c"
  }

  vpc_security_group_ids = ["sg-d6d1d6a8"]

}
