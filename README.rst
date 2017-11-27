.. image:: http://www.zipline-live.io/images/zipline_live.png
    :target: https://github.com/JaysonAlbert/zipline
    :width: 212px
    :align: center
    :alt: zipline-live

zipline-live
============

|pypi badge|
|travis status|
|appveyor status|
|Coverage Status|
|Apache License|

基于tdx的zipline.


`zipline <http://zipline.io/>`_ 是美国 `Quantopian <https://quantopian.com/>`_ 公司开源的量化交易回测引擎，它使用 ``Python`` 语言开发，
部分代码使用 ``cython`` 融合了部分c语言代码。 ``Quantopian`` 在它的网站上的回测系统就是基于 ``zipline`` 的，
经过生产环境的长期使用，已经比完善，并且在持续的改进中。

``zipline`` 的基本使用方法在 https://www.quantopian.com/tutorials/getting-started/ ,对于zipline的深度解析，可以看大神 `rainx <https://github.com/rainx>`_ 写的 `文档 <https://www.gitbook.com/book/rainx/-zipline/details>`_ ，本项目中的大部分依赖项目也都是rainx开发的项目


数据源
--------

``cn-zipline-live`` 的历史k线以及除息除权数据来自通达信，数据接口来自项目github 项目 `tdx <https://github.com/JaysonAlbert/tdx>`_

安装
----------

    pip install cn-zipline-live


实盘
----------
实盘部分代码正在测试中，请参考 `issue <https://github.com/JaysonAlbert/zipline/issues/1>`_


使用
----------

cn-zipline-live与zipline大同小异，具体使用方法请参考zipline `官方文档 <https://www.quantopian.com/tutorials/getting-started>`_ 。


一、ingest数据：
-----------

    zipline ingest -b tdx -a assets.csv --minute False --start 20170901 --overwrite True

``-a assets.csv`` 指定需要 ``ingest`` 的代码列表，缺省ingest 4000+只所有股票，耗时长达3、4小时，通过 ``-a tests/ETF.csv`` 只ingest ETF基金数据，一方面可以节省时间达到快速测试的目的。
另一方面可以通过这种方法ingest非股票数据，例如etf基金。

``--minute False`` 是否ingest分钟数据

``--start 20170901`` 数据开始日期，默认为1991年

``--overwrite True`` 由于分钟数据获取速度较慢，默认start至今超过3年的话，只拿3年数据，日线数据依然以start为准，overwrite为True时，强制拿从start开始  至今的分钟数据


二、编写策略以及运行策略：
-----------

请参考目录: ``zipline/examples``


问题
--------------

如有任何问题，欢迎大家提交 `issue <https://github.com/JaysonAlbert/zipline/issues/new/>`_ ，反馈bug，以及提出改进建议。

其它
--------------
对量化感兴趣的朋友，以及想更方便的交流朋友，请加QQ群434588628



.. |pypi badge| image:: https://badge.fury.io/py/cn-zipline-live.svg
    :target: https://pypi.python.org/pypi/cn-zipline-live
.. |travis status| image:: https://travis-ci.org/JaysonAlbert/zipline.svg?branch=master
    :target: https://travis-ci.org/JaysonAlbert/zipline
.. |appveyor status| image:: https://ci.appveyor.com/api/projects/status/fc6rgyckxj445uf5?svg=true
   :target: https://ci.appveyor.com/project/JaysonAlbert/zipline/branch/master
.. |Coverage Status| image:: https://coveralls.io/repos/github/JaysonAlbert/zipline/badge.svg?branch=master
   :target: https://coveralls.io/github/JaysonAlbert/zipline?branch=master
.. |Apache License| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
   :target: https://www.apache.org/licenses/LICENSE-2.0


.. _`Zipline Install Documentation` : http://www.zipline.io/install.html
