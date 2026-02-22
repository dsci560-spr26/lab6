CREATE USER 'labuser'@'localhost' IDENTIFIED BY 'labpass';
GRANT ALL PRIVILEGES ON lab6.* TO 'labuser'@'localhost';
FLUSH PRIVILEGES;