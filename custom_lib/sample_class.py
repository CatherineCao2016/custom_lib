class Employee:

    def __init__(self, first, last, gender):
        self.first = first
        self.last = last
        self.email = first + '.' + last + '@email.com'
        self.gender = gender

    def fullname(self):
        return '{} {}'.format(self.first, self.last)
    

    