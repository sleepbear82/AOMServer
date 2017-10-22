# -*- coding: utf-8 -*-

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
#BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
#SECRET_KEY = 'rd$6e6@s-1_0#e-nrth#v8(x5byat_1ju&z1q)$kpdv+jwfn=z'
SECRET_KEY = "ctmj#&amp;8hrgow_^sj$ejt@9fzsmh_o)-=(byt5jmg=e3#foya6u"
SYSTEM_NAME="自动化运维平台-Demo"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'stats',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [ os.path.join(BASE_DIR,  'templates')],  
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'stats.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases
'''
DATABASES = {
    'default': {
        #'ENGINE': 'django.db.backends.sqlite3',
        #'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'dcim_solr_demo',
        'USER':'postgres',
        'PASSWORD':'neusoft',
        'HOST':'10.3.51.175',
        'PORT':'5432',
    }
}
'''

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Shanghai'
USE_I18N = True
USE_L10N = True
USE_TZ = True
FILE_CHARSET = 'utf-8'
DEFAULT_CHARSET = 'utf-8'

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATICFILES_DIRS = (
    ('css',os.path.join(STATIC_ROOT,'css').replace('\\','/') ),
    ('js',os.path.join(STATIC_ROOT,'js').replace('\\','/') ),
    ('images',os.path.join(STATIC_ROOT,'images').replace('\\','/') ),
)
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
)

LOGGING = {
     'version': 1,
     'disable_existing_loggers': True,
     'formatters': {
         'simple': {
             'format': '[%(asctime)s] %(levelname)s : %(message)s'
         },
         'verbose': {
             'format': '[%(asctime)s] %(levelname)s %(module)s %(process)d %(thread)d : %(message)s'
         },
     },
     'handlers': {
         'console': {
             'level': 'INFO',
             'class': 'logging.StreamHandler',
             'formatter': 'simple',
         },
         'file': {
             'level': 'INFO',
             'class': 'logging.FileHandler',
             'formatter': 'simple',
             'filename': BASE_DIR+'/logs/sys.log',
             'mode': 'a',
         },
     },
     'loggers': {
         'django': {
             'handlers': ['file', 'console'],
             'level':'INFO',
             'propagate': True,
         },
     },
 }
