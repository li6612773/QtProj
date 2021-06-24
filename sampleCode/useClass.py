class Turtle:#python 中类名约定以大写字母开头
  '''关于类的简单例子。。。'''
  #属性 == 类变量
  color ="green"
  weight="10kg"
  legs=4
  shell=True
  mouth='big'
  #方法
  def climb(self):
    self.name = "test"  #实例变量：定义在方法中的变量，只作用于当前实例的类。
    print("我在很努力爬。")
  def run(self):
    print('我在很努力跑。')
  def bite(self):
    print('我要要要要要')
  def sleep(self):
    print('我要睡觉啦。')
#创建一个实例对象也就是类的实例化！
tt =Turtle() #类的实例化，也就是创建一个对象，类名约定大写字母开头
tt.bite() #创建好类后就能调用类里面的方法叻；
tt.sleep()


