#!/bin/bash
rm -rf /etc/pacman.d/gnupg
rm -rf /var/lib/pacman/db.lck
echo "TERM=xterm" > /etc/profile.d/term.sh
pacman-key --init
pacman-key --populate archlinux
pacman -Syu --noconfirm
pacman -S --noconfirm git base-devel go wget gnu-netcat
